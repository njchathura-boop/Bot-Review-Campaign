[CmdletBinding()]
param(
    [ValidateRange(1, 1000000)]
    [int]$Limit = 25000,

    [string]$OutputDirectory = "data/raw",

    [string]$PythonPath = "",

    [switch]$BuildBundle,

    [switch]$BuildTemporalBundle,

    [string]$BundleOutputDirectory = "data/processed/dataset_bundle",

    [ValidateRange(0, 1000000)]
    [int]$AugmentationCount = 7000,

    [ValidateRange(0, 10000000)]
    [int]$CampaignScenarioCount = 0,

    [ValidateRange(0.0, 1.0)]
    [double]$MaxSyntheticFraction = 0.25,

    [int]$Seed = 42
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Categories = @(
    "All_Beauty",
    "Amazon_Fashion",
    "Appliances",
    "Arts_Crafts_and_Sewing",
    "Automotive",
    "Baby_Products",
    "Beauty_and_Personal_Care",
    "Books",
    "CDs_and_Vinyl",
    "Cell_Phones_and_Accessories",
    "Clothing_Shoes_and_Jewelry",
    "Digital_Music",
    "Electronics",
    "Gift_Cards",
    "Grocery_and_Gourmet_Food",
    "Handmade_Products",
    "Health_and_Household",
    "Health_and_Personal_Care",
    "Home_and_Kitchen",
    "Industrial_and_Scientific",
    "Kindle_Store",
    "Magazine_Subscriptions",
    "Movies_and_TV",
    "Musical_Instruments",
    "Office_Products",
    "Patio_Lawn_and_Garden",
    "Pet_Supplies",
    "Software",
    "Sports_and_Outdoors",
    "Subscription_Boxes",
    "Tools_and_Home_Improvement",
    "Toys_and_Games",
    "Video_Games"
)

# At the pinned source revision this category contains fewer than 25,000 reviews.
# Zero-copy duplication is intentionally avoided because it would bias temporal rates.
$KnownCategoryMaximumRecords = @{
    "Subscription_Boxes" = 16216
}

function Get-TextLineCount {
    param([Parameter(Mandatory)][string]$LiteralPath)

    $Reader = [System.IO.File]::OpenText((Resolve-Path -LiteralPath $LiteralPath).Path)
    try {
        $Count = 0
        while ($null -ne $Reader.ReadLine()) {
            $Count++
        }
        return $Count
    }
    finally {
        $Reader.Dispose()
    }
}

$RepositoryRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepositoryRoot
try {
    if ([string]::IsNullOrWhiteSpace($PythonPath)) {
        $VirtualEnvironmentPython = Join-Path $RepositoryRoot ".venv/Scripts/python.exe"
        $PythonPath = if (Test-Path -LiteralPath $VirtualEnvironmentPython) {
            $VirtualEnvironmentPython
        }
        else {
            "python"
        }
    }

    $ExpectedSourceRevision = (& $PythonPath -c "from bot_campaign.amazon import DATASET_REVISION; print(DATASET_REVISION)").Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($ExpectedSourceRevision)) {
        throw "Could not read the pinned Amazon dataset revision from bot_campaign.amazon."
    }

    New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
    $TotalRecords = 0
    $ExpectedTotal = 0
    $BehavioralInputs = @()

    foreach ($Category in $Categories) {
        $FileName = "amazon_$($Category.ToLowerInvariant()).jsonl"
        $OutputPath = Join-Path $OutputDirectory $FileName
        $PartialPath = "$OutputPath.partial"
        $BehavioralInputs += $OutputPath
        $ExpectedCategoryRecords = if ($KnownCategoryMaximumRecords.ContainsKey($Category)) {
            [Math]::Min($Limit, $KnownCategoryMaximumRecords[$Category])
        }
        else {
            $Limit
        }
        $ExpectedTotal += $ExpectedCategoryRecords

        if (Test-Path -LiteralPath $OutputPath) {
            $ExistingCount = Get-TextLineCount -LiteralPath $OutputPath
            $FirstRecord = Get-Content -LiteralPath $OutputPath -TotalCount 1 | ConvertFrom-Json
            $RevisionProperty = $FirstRecord.PSObject.Properties["source_revision"]
            $ExistingRevision = if ($null -ne $RevisionProperty) {
                [string]$RevisionProperty.Value
            }
            else {
                ""
            }
            if (
                $ExistingCount -eq $ExpectedCategoryRecords -and
                $ExistingRevision -eq $ExpectedSourceRevision
            ) {
                Write-Host "Skipping ${Category}: already contains $ExistingCount records."
                $TotalRecords += $ExistingCount
                continue
            }
            Write-Host "${Category}: count or source revision is stale; downloading a replacement."
        }

        if (Test-Path -LiteralPath $PartialPath) {
            $PartialCount = Get-TextLineCount -LiteralPath $PartialPath
            $PartialFirstRecord = Get-Content -LiteralPath $PartialPath -TotalCount 1 |
                ConvertFrom-Json
            $PartialRevisionProperty = $PartialFirstRecord.PSObject.Properties["source_revision"]
            $PartialRevision = if ($null -ne $PartialRevisionProperty) {
                [string]$PartialRevisionProperty.Value
            }
            else {
                ""
            }
            if (
                $PartialCount -eq $ExpectedCategoryRecords -and
                $PartialRevision -eq $ExpectedSourceRevision
            ) {
                Move-Item -LiteralPath $PartialPath -Destination $OutputPath -Force
                $TotalRecords += $PartialCount
                Write-Host "Completed ${Category}: reused $PartialCount fully available records."
                continue
            }
        }

        Write-Host "Downloading ${Category} ($Limit records)..."
        & $PythonPath -m bot_campaign.cli download-amazon `
            --category $Category `
            --limit $Limit `
            --output $PartialPath

        if ($LASTEXITCODE -ne 0) {
            throw "Download failed for category: $Category"
        }

        $DownloadedCount = Get-TextLineCount -LiteralPath $PartialPath
        if ($DownloadedCount -ne $ExpectedCategoryRecords) {
            throw "${Category}: downloaded $DownloadedCount records; expected $ExpectedCategoryRecords. Partial file: $PartialPath"
        }

        Move-Item -LiteralPath $PartialPath -Destination $OutputPath -Force
        $TotalRecords += $DownloadedCount
        Write-Host "Completed ${Category}: $DownloadedCount records."
    }

    if ($TotalRecords -ne $ExpectedTotal) {
        throw "Downloaded total is $TotalRecords records; expected $ExpectedTotal."
    }

    Write-Host "All categories complete: $TotalRecords records across $($Categories.Count) categories."

    if ($BuildBundle -and $BuildTemporalBundle) {
        throw "Choose either -BuildBundle or -BuildTemporalBundle, not both."
    }

    if ($BuildTemporalBundle) {
        $ResolvedCampaignScenarioCount = if ($CampaignScenarioCount -eq 0) {
            $TotalRecords
        }
        else {
            $CampaignScenarioCount
        }
        $TemporalArguments = @(
            "-m",
            "bot_campaign.cli",
            "build-temporal-bundle",
            "--behavioral-input"
        )
        $TemporalArguments += $BehavioralInputs
        $TemporalArguments += @(
            "--output-dir",
            $BundleOutputDirectory,
            "--campaign-scenario-count",
            $ResolvedCampaignScenarioCount.ToString([System.Globalization.CultureInfo]::InvariantCulture),
            "--seed",
            $Seed.ToString([System.Globalization.CultureInfo]::InvariantCulture)
        )

        Write-Host "Building temporal bundle with $ResolvedCampaignScenarioCount controlled scenario records..."
        & $PythonPath $TemporalArguments
        if ($LASTEXITCODE -ne 0) {
            throw "Temporal dataset bundle generation failed."
        }
        Write-Host "Temporal dataset bundle completed: $BundleOutputDirectory"
    }

    if ($BuildBundle) {
        $ResolvedCampaignScenarioCount = if ($CampaignScenarioCount -eq 0) {
            $TotalRecords
        }
        else {
            $CampaignScenarioCount
        }
        $LabeledProductReviews = "data/raw/product_reviews.jsonl"
        $LabeledKaggleReviews = "data/raw/kaggle_fake_reviews"
        foreach ($LabeledInput in @($LabeledProductReviews, $LabeledKaggleReviews)) {
            if (-not (Test-Path -LiteralPath $LabeledInput)) {
                throw "Required labeled input does not exist: $LabeledInput"
            }
        }

        $BundleArguments = @(
            "-m",
            "bot_campaign.cli",
            "build-dataset-bundle",
            "--labeled-input",
            $LabeledProductReviews,
            $LabeledKaggleReviews,
            "--behavioral-input"
        )
        $BundleArguments += $BehavioralInputs
        $BundleArguments += @(
            "--output-dir",
            $BundleOutputDirectory,
            "--augmentation-count",
            $AugmentationCount.ToString([System.Globalization.CultureInfo]::InvariantCulture),
            "--campaign-scenario-count",
            $ResolvedCampaignScenarioCount.ToString([System.Globalization.CultureInfo]::InvariantCulture),
            "--max-synthetic-fraction",
            $MaxSyntheticFraction.ToString([System.Globalization.CultureInfo]::InvariantCulture),
            "--seed",
            $Seed.ToString([System.Globalization.CultureInfo]::InvariantCulture)
        )

        Write-Host "Building dataset bundle with $ResolvedCampaignScenarioCount controlled scenario records..."
        & $PythonPath $BundleArguments
        if ($LASTEXITCODE -ne 0) {
            throw "Dataset bundle generation failed."
        }
        Write-Host "Dataset bundle completed: $BundleOutputDirectory"
    }
}
finally {
    Pop-Location
}
