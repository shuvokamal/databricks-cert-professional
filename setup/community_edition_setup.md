# Databricks Community Edition Setup

## 1. Create Account
- Go to: https://community.cloud.databricks.com/
- Sign up for Community Edition (free, no credit card)
- Verify your email

## 2. Create a Cluster
1. Click "Compute" in the left sidebar
2. Click "Create Cluster"
3. Settings:
   - Cluster name: `cert-cluster`
   - Runtime: **14.3 LTS (Scala 2.12, Spark 3.5.0)** — latest LTS
   - Node type: default (Community gives you one free node)
   - Auto-terminate: 2 hours
4. Click "Create Cluster"

> Community clusters terminate after 2h of inactivity and take ~5 min to restart.

## 3. Import Notebooks from This Repo
1. Go to your Databricks workspace
2. Click "Workspace" in the left sidebar
3. Right-click a folder → "Import"
4. Choose "File" and upload any `.py` file from this project
5. The notebook opens ready to run

## 4. Import Official Databricks Academy Notebooks (Free)
The official DE Professional lab files are on GitHub:
- Repo: `databricks-academy/data-engineer-learning-path`
- URL: https://github.com/databricks-academy/data-engineer-learning-path

To import the whole repo into Databricks:
1. Go to "Repos" in the left sidebar
2. Click "Add Repo"
3. Paste the GitHub URL above
4. Click "Create Repo"

## 5. Limitations in Community Edition vs Professional Exam
| Feature | Community Edition | Full Workspace |
|---|---|---|
| Delta Lake | Full support | Full support |
| Structured Streaming | Full support | Full support |
| Spark Optimization | Full support | Full support |
| Databricks Workflows (Jobs) | Basic (no UI orchestration) | Full support |
| Unity Catalog | NOT available | Full support |
| Multi-node clusters | NOT available | Full support |
| DLT (Delta Live Tables) | NOT available | Full support |

**Workaround for missing features:**
- Workflows: Study the JSON job definition format + use Databricks docs
- Unity Catalog: Use the free Databricks trials on Azure/AWS just for this topic (14-day trial)
- DLT: Watch Databricks Academy videos + read the docs
