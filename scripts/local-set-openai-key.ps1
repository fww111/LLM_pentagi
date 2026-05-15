param(
    [string]$EnvPath = ".env"
)

$secureKey = Read-Host "OpenAI-compatible API key" -AsSecureString
$bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureKey)

try {
    $key = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    if ([string]::IsNullOrWhiteSpace($key)) {
        throw "API key cannot be empty"
    }

    $content = Get-Content -LiteralPath $EnvPath -Raw
    $content = $content -replace "(?m)^OPEN_AI_KEY=.*$", "OPEN_AI_KEY=$key"
    $content = $content -replace "(?m)^EMBEDDING_KEY=.*$", "EMBEDDING_KEY=$key"
    Set-Content -LiteralPath $EnvPath -Value $content -NoNewline
    Write-Host "Updated $EnvPath"
} finally {
    if ($bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}
