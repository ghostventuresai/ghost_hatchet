//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createUserSessionRepository(pool *pgxpool.Pool) *userSessionRepository {
	logger := zerolog.Nop()
	shared := &sharedRepository{
		pool:    pool,
		ddlPool: pool,
		l:       &logger,
		queries: sqlcv1.New(),
	}
	return &userSessionRepository{
		sharedRepository: shared,
	}
}

func createTestUser(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	userId := uuid.New()
	_, err := pool.Exec(ctx(t), `
		INSERT INTO "User" ("id", "email", "emailVerified", "name", "createdAt", "updatedAt")
		VALUES ($1, $2, false, $3, NOW(), NOW())
	`, userId, userId.String()+"@test.com", "Test User")
	require.NoError(t, err)
	return userId
}

func sessionExists(t *testing.T, pool *pgxpool.Pool, sessionId uuid.UUID) bool {
	var exists bool
	err := pool.QueryRow(ctx(t), `SELECT EXISTS(SELECT 1 FROM "UserSession" WHERE "id" = $1)`, sessionId).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func countExistingSessions(t *testing.T, pool *pgxpool.Pool, sessionIds []uuid.UUID) int {
	if len(sessionIds) == 0 {
		return 0
	}

	var count int
	err := pool.QueryRow(ctx(t), `SELECT COUNT(*) FROM "UserSession" WHERE "id" = ANY($1::uuid[])`, sessionIds).Scan(&count)
	require.NoError(t, err)

	return count
}

// TestCleanupUserSessions verifies the cleanup logic handles both conditions:
// 1. Expired sessions (expiresAt < NOW())
// 2. Unauthenticated sessions (userId IS NULL) older than 24 hours
func TestCleanupUserSessions(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	testUserId := createTestUser(t, pool)
	repo := createUserSessionRepository(pool)

	// Empty table returns no errors and zero count
	deletedCount, err := repo.CleanupUserSessions(ctx(t))
	require.NoError(t, err)
	assert.Equal(t, 0, deletedCount)

	// Define test cases covering all cleanup conditions
	testCases := []struct {
		name         string
		expiresAt    time.Time
		userId       *uuid.UUID
		ageHours     int
		shouldDelete bool
	}{
		// Condition 1: Expired sessions
		{
			name:         "expired-session-with-user",
			expiresAt:    time.Now().UTC().Add(-1 * time.Hour),
			userId:       &testUserId,
			ageHours:     0,
			shouldDelete: true,
		},
		{
			name:         "just-expired-session",
			expiresAt:    time.Now().UTC().Add(-1 * time.Second),
			userId:       &testUserId,
			ageHours:     0,
			shouldDelete: true,
		},

		// Condition 2: Unauthenticated and old sessions
		{
			name:         "unauthenticated-old-session",
			expiresAt:    time.Now().UTC().Add(48 * time.Hour),
			userId:       nil,
			ageHours:     25,
			shouldDelete: true,
		},

		// Cases that should NOT be deleted
		{
			name:         "valid-session-with-user",
			expiresAt:    time.Now().UTC().Add(24 * time.Hour),
			userId:       &testUserId,
			ageHours:     0,
			shouldDelete: false,
		},
		{
			name:         "unauthenticated-recent-session",
			expiresAt:    time.Now().UTC().Add(48 * time.Hour),
			userId:       nil,
			ageHours:     1,
			shouldDelete: false,
		},
		{
			name:         "session-expires-in-1-second",
			expiresAt:    time.Now().UTC().Add(1 * time.Second),
			userId:       &testUserId,
			ageHours:     0,
			shouldDelete: false,
		},
	}

	// Insert all test sessions
	sessionIds := make(map[string]uuid.UUID)
	for _, tc := range testCases {
		sessionId := uuid.New()
		sessionIds[tc.name] = sessionId

		var err error
		if tc.ageHours > 0 {
			_, err = pool.Exec(ctx(t), `
				INSERT INTO "UserSession" ("id", "expiresAt", "userId", "data", "createdAt", "updatedAt")
				VALUES ($1, $2 AT TIME ZONE 'UTC', $3, '{}', NOW() - INTERVAL '1 hour' * $4, NOW() - INTERVAL '1 hour' * $4)
			`, sessionId, tc.expiresAt, tc.userId, tc.ageHours)
		} else {
			_, err = pool.Exec(ctx(t), `
				INSERT INTO "UserSession" ("id", "expiresAt", "userId", "data", "createdAt", "updatedAt")
				VALUES ($1, $2 AT TIME ZONE 'UTC', $3, '{}', NOW(), NOW())
			`, sessionId, tc.expiresAt, tc.userId)
		}
		require.NoError(t, err, "failed to insert session: %s", tc.name)
	}

	// Run cleanup
	deletedCount, err = repo.CleanupUserSessions(ctx(t))
	require.NoError(t, err)

	// Calculate expected delete count
	expectedDeletes := 0
	for _, tc := range testCases {
		if tc.shouldDelete {
			expectedDeletes++
		}
	}
	assert.Equal(t, expectedDeletes, deletedCount, "deleted count should match expected")

	// Verify each case by checking DB state
	for _, tc := range testCases {
		sessionId := sessionIds[tc.name]
		exists := sessionExists(t, pool, sessionId)

		if tc.shouldDelete {
			assert.False(t, exists, "session '%s' should be deleted from DB", tc.name)
		} else {
			assert.True(t, exists, "session '%s' should exist in DB", tc.name)
		}
	}
}

// TestCleanupUserSessions_MultiBatch verifies cleanup works correctly when
// there are more rows than the batch size (1000), forcing multiple batches.
func TestCleanupUserSessions_MultiBatch(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	testUserId := createTestUser(t, pool)
	repo := createUserSessionRepository(pool)

	// Insert 1500 expired sessions (should require 2 batches: 1000 + 500)
	const totalSessions = 1500
	sessionIds := make([]uuid.UUID, totalSessions)

	for i := 0; i < totalSessions; i++ {
		sessionIds[i] = uuid.New()
		_, err := pool.Exec(ctx(t), `
			INSERT INTO "UserSession" ("id", "expiresAt", "userId", "data", "createdAt", "updatedAt")
			VALUES ($1, $2 AT TIME ZONE 'UTC', $3, '{}', NOW(), NOW())
		`, sessionIds[i], time.Now().UTC().Add(-1*time.Hour), testUserId)
		require.NoError(t, err)
	}

	// Run cleanup
	deletedCount, err := repo.CleanupUserSessions(ctx(t))
	require.NoError(t, err)
	assert.Equal(t, totalSessions, deletedCount, "all 1500 sessions should be deleted across multiple batches")

	// Verify all sessions are deleted using batch query
	existingCount := countExistingSessions(t, pool, sessionIds)
	assert.Equal(t, 0, existingCount, "all %d sessions should be deleted", totalSessions)
}

// TestCleanupUserSessions_ExactBatchBoundary verifies cleanup works correctly
// when the number of rows equals the batch size exactly.
func TestCleanupUserSessions_ExactBatchBoundary(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	testUserId := createTestUser(t, pool)
	repo := createUserSessionRepository(pool)

	// Insert exactly 1000 expired sessions (exactly 1 batch)
	const totalSessions = 1000
	sessionIds := make([]uuid.UUID, totalSessions)

	for i := 0; i < totalSessions; i++ {
		sessionIds[i] = uuid.New()
		_, err := pool.Exec(ctx(t), `
			INSERT INTO "UserSession" ("id", "expiresAt", "userId", "data", "createdAt", "updatedAt")
			VALUES ($1, $2 AT TIME ZONE 'UTC', $3, '{}', NOW(), NOW())
		`, sessionIds[i], time.Now().UTC().Add(-1*time.Hour), testUserId)
		require.NoError(t, err)
	}

	// Run cleanup
	deleted, err := repo.CleanupUserSessions(ctx(t))
	require.NoError(t, err)
	assert.Equal(t, totalSessions, deleted, "all 1000 sessions should be deleted in single batch")

	// Verify all sessions are deleted using batch query
	existingCount := countExistingSessions(t, pool, sessionIds)
	assert.Equal(t, 0, existingCount, "all %d sessions should be deleted", totalSessions)
}

// Helper functions

func ctx(t *testing.T) context.Context {
	t.Helper()
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
