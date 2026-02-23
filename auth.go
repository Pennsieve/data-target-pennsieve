package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	cidptypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
)

// RefreshAndGetAWSCredentials exchanges a Cognito refresh token for temporary
// AWS credentials that can be used to upload to S3.
//
// Flow:
//  1. REFRESH_TOKEN_AUTH via Cognito UserPool → IdToken
//  2. GetId + GetCredentialsForIdentity via Cognito Identity Pool → AWS creds
func RefreshAndGetAWSCredentials(ctx context.Context, cognitoConfig *CognitoConfig, refreshToken string) (aws.Credentials, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cognitoConfig.Region))
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("loading AWS config: %w", err)
	}

	// Step 1: Refresh token → IdToken using UserPool.AppClientID
	idToken, err := refreshForIDToken(ctx, cfg, cognitoConfig, refreshToken)
	if err != nil {
		return aws.Credentials{}, err
	}

	// Step 2: IdToken → temporary AWS credentials using Identity Pool
	creds, err := getCredentialsForIdentity(ctx, cfg, cognitoConfig, idToken)
	if err != nil {
		return aws.Credentials{}, err
	}

	return creds, nil
}

// refreshForIDToken uses the Cognito REFRESH_TOKEN_AUTH flow with the
// UserPool's AppClientID to get a fresh IdToken.
func refreshForIDToken(ctx context.Context, cfg aws.Config, cognitoConfig *CognitoConfig, refreshToken string) (string, error) {
	svc := cognitoidentityprovider.NewFromConfig(cfg)

	resp, err := svc.InitiateAuth(ctx, &cognitoidentityprovider.InitiateAuthInput{
		AuthFlow: cidptypes.AuthFlowTypeRefreshToken,
		ClientId: aws.String(cognitoConfig.UserPool.AppClientID),
		AuthParameters: map[string]string{
			"REFRESH_TOKEN": refreshToken,
		},
	})
	if err != nil {
		return "", fmt.Errorf("REFRESH_TOKEN_AUTH failed: %w", err)
	}

	if resp.AuthenticationResult == nil || resp.AuthenticationResult.IdToken == nil {
		return "", fmt.Errorf("REFRESH_TOKEN_AUTH returned no IdToken")
	}

	return *resp.AuthenticationResult.IdToken, nil
}

// getCredentialsForIdentity exchanges an IdToken for temporary AWS credentials
// via the Cognito Identity Pool. Uses TokenPool.ID as the login provider key.
func getCredentialsForIdentity(ctx context.Context, cfg aws.Config, cognitoConfig *CognitoConfig, idToken string) (aws.Credentials, error) {
	svc := cognitoidentity.NewFromConfig(cfg)

	// The logins key must match the issuer of the IdToken, which is the UserPool
	poolResource := fmt.Sprintf("cognito-idp.%s.amazonaws.com/%s", cognitoConfig.UserPool.Region, cognitoConfig.UserPool.ID)
	logins := map[string]string{
		poolResource: idToken,
	}

	// GetId: exchange IdToken for an identity ID
	idResp, err := svc.GetId(ctx, &cognitoidentity.GetIdInput{
		IdentityPoolId: aws.String(cognitoConfig.IdentityPool.ID),
		Logins:         logins,
	})
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("Cognito GetId failed: %w", err)
	}

	// GetCredentialsForIdentity: exchange identity ID for temporary AWS creds
	credResp, err := svc.GetCredentialsForIdentity(ctx, &cognitoidentity.GetCredentialsForIdentityInput{
		IdentityId: idResp.IdentityId,
		Logins:     logins,
	})
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("Cognito GetCredentialsForIdentity failed: %w", err)
	}

	if credResp.Credentials == nil {
		return aws.Credentials{}, fmt.Errorf("GetCredentialsForIdentity returned no credentials")
	}

	return aws.Credentials{
		AccessKeyID:     *credResp.Credentials.AccessKeyId,
		SecretAccessKey:  *credResp.Credentials.SecretKey,
		SessionToken:    *credResp.Credentials.SessionToken,
		CanExpire:       true,
		Expires:         *credResp.Credentials.Expiration,
	}, nil
}
