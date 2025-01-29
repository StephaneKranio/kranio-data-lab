# README #

### What is this repository for? ###

* Script files of different languages (python, sql, scala...) and with different libraries (pandas, pyspark, ...) that contain business logic that is applied in different data flows/pipelines.

### Features GIT Flow ###

1. Create a `feat` branch from `development` branch
2. Do a pull request to `development`
3. Do a squash and merge to `development`
4. Create a `release` branch from development
5. Use branch name with thins format: `release/{year}.{month}.{day}.{correlative_num}`
6. Do a merge request to `production`
7. Do a merge to `production`

### Commits Names Guideliens ###

### Branches Names Guideline ###

### Local Pre-Commit Usage ###

1. Install pre-commit python library: `pip install pre-commit`
2. install pre-commit hooks in the repo directory: `pre-commit install`

- Run pre-commit over all repo files: `pre-commit run --all-files`
- Run pre-commit over all staged files: `pre-commit run`

### Fill Workflows Parameters
1. To fill workflows parameters, you cant run in cmd:
  - DEV: Linux command `export STAGE=dev`
         Windows command `set STAGE=dev`
  - PRD: Linux command `export STAGE=prd`
         Windows command `set STAGE=prd`
2. and after run in folder __ scripts __: `python3 fill_workflow_params.py`
---

Build commands
Run in the Dockerfile directory.


IMAGE=gcr.io/my-project/my-image:1.0.1

\# Download the BigQuery connector.
gsutil cp \
  gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar .

\# Download the Miniconda3 installer.
wget https://repo.anaconda.com/miniconda/Miniconda3-py39_4.10.3-Linux-x86_64.sh

\# Python module example
cat >test_util.py <<EOF
def hello(name):
  print("hello {}".format(name))

def read_lines(path):
  with open(path) as f:
    return f.readlines()
EOF

\# Build and push the image.
docker build -t "${IMAGE}" .
docker push "${IMAGE}"

