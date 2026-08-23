# Detectra Kafka + Spark demo

Place this folder at `k8s/streaming-demo/`.

Start stack:

```powershell
kubectl apply -k k8s/streaming-demo
kubectl rollout status deployment/detectra-spark -n bot-campaign
```

Trigger demo:

```powershell
kubectl delete job coordinated-campaign-demo -n bot-campaign --ignore-not-found
kubectl apply -f k8s/streaming-demo/demo-campaign.yaml
kubectl logs -n bot-campaign job/coordinated-campaign-demo -f
```

Optional scorer after campaign model exists:

```powershell
kubectl apply -f k8s/streaming-demo/campaign-scorer.yaml
```
