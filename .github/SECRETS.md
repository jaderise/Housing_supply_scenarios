# Required GitHub Actions Secrets

Set these in **Settings → Secrets and variables → Actions → New repository secret**
before running the `Fetch & Rebuild Data` workflow.

| Secret name    | Where to get it |
|----------------|----------------|
| `FRED_API_KEY` | https://fred.stlouisfed.org/docs/api/api_key.html — free account |
| `CENSUS_API_KEY` | https://api.census.gov/data/key_signup.html — free, instant |
| `HUD_API_KEY`  | https://www.huduser.gov/hudapi/public/register — free account |

The `validate_pipeline.yml` workflow uses stub values and **does not** require
these secrets — it only needs them for the live fetch in `fetch_data.yml`.
