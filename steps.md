downloads the data
behaviour generation
campaign also generated
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\scripts\download_amazon_categories.ps1" -Limit 25000 -BuildTemporalBundle -CampaignScenarioCount 0 -BundleOutputDirectory "data/processed/temporal_bundle"

docker compose up -d mlflow

python training/ray_train.py `
  --smoke `
  --gpus-per-trial 0

  docker compose up -d kafka spark-master spark-worker

docker compose --profile stream up -d spark-stream

docker compose --profile score up -d campaign-scorer

python streaming/producer.py `
  --input data/processed/temporal_bundle/campaign/test.jsonl `
  --bootstrap-servers localhost:9092 `
  --rate 100

  docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:29092 `
  --topic reviews.campaign-scores.v1 `
  --from-beginning `
  --max-messages 5

  docker compose ps

docker compose logs --tail 100 spark-stream campaign-scorer

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka:29092 `
  --describe `
  --group campaign-scorer-v1