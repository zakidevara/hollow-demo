# ============================================================
# Kafka KTable Auto-Refresh Demo Script
# ============================================================
# This script demonstrates how Kafka KTables provide automatic
# refresh without manual intervention (unlike Hollow file watchers).
#
# Prerequisites:
# 1. Start Kafka: docker-compose -f docker-compose-kafka.yml up -d
# 2. Start app: ./gradlew bootRun --args='--spring.profiles.active=kafka'
# ============================================================

$baseUrl = "http://localhost:8080/api/kafka"

function Write-ColorText {
    param([string]$Text, [string]$Color = "White")
    Write-Host $Text -ForegroundColor $Color
}

function Invoke-ApiCall {
    param([string]$Method, [string]$Uri, [object]$Body = $null)
    try {
        $params = @{
            Method = $Method
            Uri = $Uri
            ContentType = "application/json"
        }
        if ($Body) {
            $params.Body = ($Body | ConvertTo-Json -Depth 10)
        }
        $response = Invoke-RestMethod @params
        return $response
    }
    catch {
        Write-ColorText "Error: $($_.Exception.Message)" "Red"
        return $null
    }
}

# ============================================================
# Demo Start
# ============================================================
Write-ColorText "`n╔══════════════════════════════════════════════════════════╗" "Cyan"
Write-ColorText "║          Kafka KTable Auto-Refresh Demo                  ║" "Cyan"
Write-ColorText "╚══════════════════════════════════════════════════════════╝" "Cyan"

# Check status
Write-ColorText "`n[1/5] Checking KTable status..." "Yellow"
$status = Invoke-ApiCall -Method GET -Uri "$baseUrl/status"
if ($status) {
    Write-ColorText "State: $($status.state)" "Green"
    Write-ColorText "Ready: $($status.ready)" "Green"
    if ($status.approximateCount) {
        Write-ColorText "Records: $($status.approximateCount)" "Green"
    }
}

# Publish initial data
Write-ColorText "`n[2/5] Publishing initial users..." "Yellow"
$users = @(
    @{ id = 100; username = "kafka_user_1"; active = $true },
    @{ id = 101; username = "kafka_user_2"; active = $true },
    @{ id = 102; username = "kafka_user_3"; active = $false }
)
$result = Invoke-ApiCall -Method POST -Uri "$baseUrl/publish" -Body $users
if ($result) {
    Write-ColorText "Published $($result.recordCount) records" "Green"
    Write-ColorText "Note: $($result.note)" "Cyan"
}

Start-Sleep -Seconds 1

# Query without refresh
Write-ColorText "`n[3/5] Querying users (NO REFRESH NEEDED - auto-updated!)..." "Yellow"
$allUsers = Invoke-ApiCall -Method GET -Uri "$baseUrl/users"
if ($allUsers) {
    Write-ColorText "Found $($allUsers.Count) users:" "Green"
    $allUsers | ForEach-Object {
        $activeIcon = if ($_.active) { "✓" } else { "✗" }
        Write-ColorText "  [$activeIcon] ID: $($_.id), Username: $($_.username)" "White"
    }
}

# Publish update
Write-ColorText "`n[4/5] Publishing update (user 101 now inactive)..." "Yellow"
$updatedUser = @{ id = 101; username = "kafka_user_2_updated"; active = $false }
$result = Invoke-ApiCall -Method POST -Uri "$baseUrl/users" -Body $updatedUser
if ($result) {
    Write-ColorText "Updated user: $($result.user.username)" "Green"
}

Start-Sleep -Milliseconds 500

# Query again - should see update without refresh
Write-ColorText "`n[5/5] Querying again (should see update - NO REFRESH CALLED!)..." "Yellow"
$user = Invoke-ApiCall -Method GET -Uri "$baseUrl/users/101"
if ($user) {
    Write-ColorText "User 101 now:" "Green"
    Write-ColorText "  Username: $($user.username)" "White"
    Write-ColorText "  Active: $($user.active)" "White"
}

# Summary
Write-ColorText "`n╔══════════════════════════════════════════════════════════╗" "Cyan"
Write-ColorText "║                      Demo Complete!                       ║" "Cyan"
Write-ColorText "╠══════════════════════════════════════════════════════════╣" "Cyan"
Write-ColorText "║  Key Difference from Hollow:                             ║" "Cyan"
Write-ColorText "║  • Hollow: Requires triggerRefresh() or file watcher     ║" "White"
Write-ColorText "║  • Kafka KTable: Auto-updates as records stream in!      ║" "Green"
Write-ColorText "╚══════════════════════════════════════════════════════════╝" "Cyan"

Write-ColorText "`nAvailable endpoints:" "Yellow"
Write-ColorText "  GET  $baseUrl/users         - List all users" "White"
Write-ColorText "  GET  $baseUrl/users/{id}    - Get user by ID" "White"
Write-ColorText "  POST $baseUrl/users         - Create/update user" "White"
Write-ColorText "  DELETE $baseUrl/users/{id}  - Delete user" "White"
Write-ColorText "  GET  $baseUrl/status        - Check KTable status" "White"
