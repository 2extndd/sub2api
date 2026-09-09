package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func openAIAPIKeyWithCapabilities(capabilities ...string) *Account {
	credentials := map[string]any{"api_key": "sk-test"}
	if capabilities != nil {
		credentials[openAIEndpointCapabilitiesCredentialKey] = capabilities
	}
	return &Account{
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Credentials: credentials,
	}
}

func TestSupportsOpenAIEndpointCapability_Images(t *testing.T) {
	t.Run("unmarked API key remains text compatible but cannot generate images", func(t *testing.T) {
		account := openAIAPIKeyWithCapabilities()

		require.True(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityChatCompletions))
		require.True(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityEmbeddings))
		require.False(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityImages))
	})

	t.Run("image-only API key cannot receive text", func(t *testing.T) {
		account := openAIAPIKeyWithCapabilities("images")

		require.True(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityImages))
		require.False(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityChatCompletions))
		require.False(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityEmbeddings))
		require.False(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityLive))
	})

	t.Run("mixed API key explicitly supports text and images", func(t *testing.T) {
		account := openAIAPIKeyWithCapabilities("chat_completions", "images")

		require.True(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityChatCompletions))
		require.True(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityImages))
	})

	t.Run("OAuth preserves implicit image support", func(t *testing.T) {
		account := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}

		require.True(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityImages))
	})

	t.Run("non-OpenAI account is rejected", func(t *testing.T) {
		account := &Account{Platform: PlatformGrok, Type: AccountTypeOAuth}

		require.False(t, account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityImages))
	})
}

func TestSupportsOpenAIImageCapability_RequiresExplicitAPIKeyOptIn(t *testing.T) {
	t.Run("unmarked API key is rejected from direct Images scheduling", func(t *testing.T) {
		account := openAIAPIKeyWithCapabilities()

		require.False(t, account.SupportsOpenAIImageCapability(OpenAIImagesCapabilityBasic))
		require.False(t, account.SupportsOpenAIImageCapability(OpenAIImagesCapabilityNative))
	})

	t.Run("image-only API key supports basic and native image requests", func(t *testing.T) {
		account := openAIAPIKeyWithCapabilities("images")

		require.True(t, account.SupportsOpenAIImageCapability(OpenAIImagesCapabilityBasic))
		require.True(t, account.SupportsOpenAIImageCapability(OpenAIImagesCapabilityNative))
	})

	t.Run("OAuth keeps basic and native image support", func(t *testing.T) {
		account := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}

		require.True(t, account.SupportsOpenAIImageCapability(OpenAIImagesCapabilityBasic))
		require.True(t, account.SupportsOpenAIImageCapability(OpenAIImagesCapabilityNative))
	})
}

func TestAccountSupportsOpenAICapabilities_ResponsesImageIntentRequiresBoth(t *testing.T) {
	responsesExtra := map[string]any{"openai_responses_supported": true}

	t.Run("unmarked Responses-capable API key fails closed", func(t *testing.T) {
		account := openAIAPIKeyWithCapabilities()
		account.Extra = responsesExtra

		require.False(t, accountSupportsOpenAICapabilities(
			account,
			OpenAIEndpointCapabilityResponses,
			OpenAIImagesCapabilityBasic,
		))
	})

	t.Run("chat-only Responses-capable API key fails closed", func(t *testing.T) {
		account := openAIAPIKeyWithCapabilities("chat_completions")
		account.Extra = responsesExtra

		require.False(t, accountSupportsOpenAICapabilities(
			account,
			OpenAIEndpointCapabilityResponses,
			OpenAIImagesCapabilityBasic,
		))
	})

	t.Run("image-only API key still lacks the text Responses endpoint", func(t *testing.T) {
		account := openAIAPIKeyWithCapabilities("images")
		account.Extra = responsesExtra

		require.False(t, accountSupportsOpenAICapabilities(
			account,
			OpenAIEndpointCapabilityResponses,
			OpenAIImagesCapabilityBasic,
		))
	})

	t.Run("API key with Responses and images passes", func(t *testing.T) {
		account := openAIAPIKeyWithCapabilities("chat_completions", "images")
		account.Extra = responsesExtra

		require.True(t, accountSupportsOpenAICapabilities(
			account,
			OpenAIEndpointCapabilityResponses,
			OpenAIImagesCapabilityBasic,
		))
	})

	t.Run("normal text scheduling remains compatible for unmarked API keys", func(t *testing.T) {
		account := openAIAPIKeyWithCapabilities()

		require.True(t, accountSupportsOpenAICapabilities(
			account,
			OpenAIEndpointCapabilityChatCompletions,
			"",
		))
	})

	t.Run("direct Images scheduling accepts only opted-in API keys", func(t *testing.T) {
		unmarked := openAIAPIKeyWithCapabilities()
		imageOnly := openAIAPIKeyWithCapabilities("images")

		require.False(t, accountSupportsOpenAICapabilities(
			unmarked,
			"",
			OpenAIImagesCapabilityNative,
		))
		require.True(t, accountSupportsOpenAICapabilities(
			imageOnly,
			"",
			OpenAIImagesCapabilityNative,
		))
	})
}

func TestSelectAccountWithSchedulerForImages_StickyDBRecheckRejectsRemovedCapability(t *testing.T) {
	ctx := context.Background()
	groupID := int64(10105)
	imageCredentials := map[string]any{
		"api_key":                               "sk-image-test",
		openAIEndpointCapabilitiesCredentialKey: []string{"images"},
	}
	staleSticky := &Account{
		ID:          35001,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Priority:    0,
		GroupIDs:    []int64{groupID},
		Credentials: imageCredentials,
	}
	staleBackup := &Account{
		ID:          35002,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Priority:    5,
		GroupIDs:    []int64{groupID},
		Credentials: imageCredentials,
	}
	dbSticky := *staleSticky
	dbSticky.Credentials = map[string]any{"api_key": "sk-text-test"}
	dbBackup := *staleBackup

	cache := &schedulerTestGatewayCache{
		sessionBindings: map[string]int64{"openai:image_sticky_db_recheck": staleSticky.ID},
	}
	snapshotCache := &openAISnapshotCacheStub{
		snapshotAccounts: []*Account{staleSticky, staleBackup},
		accountsByID: map[int64]*Account{
			staleSticky.ID: staleSticky,
			staleBackup.ID: staleBackup,
		},
	}
	svc := &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: []Account{dbSticky, dbBackup}},
		cache:              cache,
		cfg:                &config.Config{},
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true"),
		schedulerSnapshot:  &SchedulerSnapshotService{cache: snapshotCache},
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
	}

	selection, _, err := svc.SelectAccountWithSchedulerForImages(
		ctx,
		&groupID,
		"image_sticky_db_recheck",
		"gpt-image-1",
		nil,
		OpenAIImagesCapabilityBasic,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.NotNil(t, selection.Account)
	require.Equal(t, staleBackup.ID, selection.Account.ID)
}
