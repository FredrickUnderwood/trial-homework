package generator

// Note: All bid/impression generation logic has been moved to client.go
// This file is kept for potential future extensions

// Corner cases implemented in client.go:
// 
// Bid corner cases:
// 1. Invalid/empty user_idfv
// 2. Zero timestamp  
// 3. Very old timestamp (1 year ago)
// 4. Future timestamp
// 5. Missing app_bundle (empty)
//
// Impression corner cases:
// 1. Invalid bid ID - random non-existent ID
// 2. Duplicate bid ID (same bid used twice)
// 3. Missing bid ID
// 4. Wrong bid ID format
