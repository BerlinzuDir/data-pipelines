cd data-pipelines
git checkout main
git stash
git reset --hard origin/main
git secret reveal -f
cd services/airflow
mv .env.prod .env
make build
make setup
bash wait-for-healthy-container.sh airflow-scheduler 300

