Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Read-SecretPlaintext {
  param([Parameter(Mandatory = $true)][string]$Prompt)

  $secure = Read-Host $Prompt -AsSecureString
  $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
  try {
    return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
  } finally {
    if ($bstr -ne [IntPtr]::Zero) {
      [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
  }
}

function Put-Secret {
  param(
    [Parameter(Mandatory = $true)][string]$Config,
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][string]$Value
  )

  if ([string]::IsNullOrWhiteSpace($Value)) {
    Write-Host "Skipping $Name for $Config"
    return
  }

  $Value | npx.cmd wrangler secret put $Name --config $Config
}

Write-Host "Staging secrets uploader"
Write-Host "Leave a value blank and press Enter to skip it."
Write-Host ""

$stripeSecret = Read-SecretPlaintext "Stripe TEST secret key (sk_test_...)"
$stripeWebhook = Read-SecretPlaintext "Stripe TEST webhook signing secret (whsec_...)"
$awsAccessKey = Read-SecretPlaintext "AWS access key id"
$awsSecretKey = Read-SecretPlaintext "AWS secret access key"
$postmarkToken = Read-SecretPlaintext "Postmark server token"
$ivsProxySecret = Read-SecretPlaintext "IVS proxy secret"

Write-Host ""
Write-Host "Uploading API Worker staging secrets..."
Put-Secret -Config "api/wrangler.staging.jsonc" -Name "STRIPE_SECRET_KEY" -Value $stripeSecret
Put-Secret -Config "api/wrangler.staging.jsonc" -Name "STRIPE_WEBHOOK_SECRET" -Value $stripeWebhook
Put-Secret -Config "api/wrangler.staging.jsonc" -Name "AWS_ACCESS_KEY_ID" -Value $awsAccessKey
Put-Secret -Config "api/wrangler.staging.jsonc" -Name "AWS_SECRET_ACCESS_KEY" -Value $awsSecretKey
Put-Secret -Config "api/wrangler.staging.jsonc" -Name "POSTMARK_SERVER_TOKEN" -Value $postmarkToken
Put-Secret -Config "api/wrangler.staging.jsonc" -Name "IVS_PROXY_SECRET" -Value $ivsProxySecret

Write-Host ""
Write-Host "Uploading Recording Worker staging secrets..."
Put-Secret -Config "recording-worker/wrangler.staging.jsonc" -Name "AWS_ACCESS_KEY_ID" -Value $awsAccessKey
Put-Secret -Config "recording-worker/wrangler.staging.jsonc" -Name "AWS_SECRET_ACCESS_KEY" -Value $awsSecretKey
Put-Secret -Config "recording-worker/wrangler.staging.jsonc" -Name "POSTMARK_SERVER_TOKEN" -Value $postmarkToken

Write-Host ""
Write-Host "Done. Run these to verify names only:"
Write-Host "npx.cmd wrangler secret list --config api/wrangler.staging.jsonc"
Write-Host "npx.cmd wrangler secret list --config recording-worker/wrangler.staging.jsonc"
