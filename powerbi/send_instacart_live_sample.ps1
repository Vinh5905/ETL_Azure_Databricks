#Requires -Version 7.0
param(
    [switch]$Send,
    [ValidateRange(1, 2147483647)][int]$Orders = 2,
    [ValidateRange(1, 2147483647)][int]$Items = 5,
    [DateTimeOffset]$Minute = [DateTimeOffset]::UtcNow
)

$ErrorActionPreference = 'Stop'
if ($Items -lt $Orders) {
    throw 'Số món mẫu phải ít nhất bằng số đơn.'
}

$minuteUtc = $Minute.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm':00Z'")
$body = ConvertTo-Json -InputObject @(@{
    minute = $minuteUtc
    orders = $Orders
    items = $Items
}) -Compress

if (-not $Send) {
    Write-Output $body
    return
}

$secret = Read-Host 'Push URL của instacart_rt' -AsSecureString
try {
    $endpoint = [System.Net.NetworkCredential]::new('', $secret).Password
    $uri = $null
    if (-not [Uri]::TryCreate($endpoint, [UriKind]::Absolute, [ref]$uri) -or
        $uri.Scheme -ne 'https' -or $uri.Host -ne 'api.powerbi.com' -or
        -not $uri.IsDefaultPort -or $uri.UserInfo -or
        $uri.AbsolutePath -notmatch '^/beta/[^/]+/datasets/[^/]+/rows/?$' -or
        $uri.Query -notmatch '[?&]key=[^&]+') {
        throw 'Push URL phải là endpoint HTTPS Power BI có key được Service tạo.'
    }

    try {
        $response = Invoke-WebRequest -Uri $uri -Method Post -ContentType 'application/json' `
            -Body $body -TimeoutSec 10 -MaximumRedirection 0 -SkipHttpErrorCheck
    } catch {
        throw 'Chưa xác định được kết quả gửi. Đối chiếu tile trước khi gửi lại.'
    }
    if ($response.StatusCode -lt 200 -or $response.StatusCode -ge 300) {
        throw "Power BI trả HTTP $($response.StatusCode)."
    }
    Write-Output "HTTP $($response.StatusCode), minute=$minuteUtc, orders=$Orders, items=$Items"
} finally {
    $secret.Dispose()
    Remove-Variable endpoint, uri -ErrorAction SilentlyContinue
}
