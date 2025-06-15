.PHONY: help install shell sftp-server s3-server monitoring run-streamer clean-all status

help:
	@echo "SFTP to S3 Streaming - Available Commands:"
	@echo ""
	@echo "Infrastructure:"
	@echo "  sftp-server    - Deploy SFTP server to Kubernetes"
	@echo "  s3-server      - Deploy MinIO S3 server to Kubernetes" 
	@echo "  monitoring     - Deploy Grafana and Prometheus to Kubernetes"
	@echo "  status         - Check status of all deployments"
	@echo ""
	@echo "Application:"
	@echo "  install        - Install Python dependencies with Poetry"
	@echo "  shell          - Activate Poetry shell"
	@echo "  run-streamer   - Install deps and run the streaming application"
	@echo ""
	@echo "Cleanup:"
	@echo "  clean-all      - Remove all Kubernetes deployments"
	@echo ""
	@echo "Quick Start:"
	@echo "  make sftp-server s3-server monitoring run-streamer"

install:
	@echo "📦 Installing Python dependencies with Poetry..."
	poetry install

shell:
	@echo "🐚 Activating Poetry shell..."
	poetry shell

sftp-server:
	@echo "🚀 Deploying SFTP server..."
	kubectl apply -f k8s/sftp/namespace.yml
	kubectl apply -f k8s/sftp/pvc.yml
	kubectl apply -f k8s/sftp/deployment.yml
	kubectl apply -f k8s/sftp/service.yml
	kubectl apply -f k8s/sftp/nodeport-service.yml
	@echo "✅ SFTP server deployed"
	@echo "📍 Access SFTP at: localhost:30022"

s3-server:
	@echo "🚀 Deploying MinIO S3 server..."
	kubectl apply -f k8s/s3/namespace.yml
	kubectl apply -f k8s/s3/pvc.yml
	kubectl apply -f k8s/s3/deployment.yml
	kubectl apply -f k8s/s3/service.yml
	@echo "✅ MinIO S3 server deployed"
	@echo "📍 Access MinIO console at: localhost:9001"
	@echo "📍 S3 API endpoint: localhost:9000"

monitoring:
	@echo "🚀 Deploying monitoring stack (Prometheus + Grafana)..."
	kubectl apply -f k8s/monitoring/namespace.yml
	kubectl apply -f k8s/monitoring/prometheus.yaml
	kubectl apply -f k8s/monitoring/grafana.yaml
	@echo "✅ Monitoring stack deployed"
	@echo "📍 Access Grafana at: localhost:3000"
	@echo "📍 Access Prometheus at: localhost:9090"
	@echo "📊 Import dashboard from: k8s/monitoring/dashboards/sftp-s3-streaming.json"

status:
	@echo "📊 Checking deployment status..."
	@echo ""
	@echo "SFTP Server:"
	kubectl get pods -n sftp-ns 2>/dev/null || echo "  ❌ SFTP namespace not found"
	@echo ""
	@echo "S3 Server:"
	kubectl get pods -n s3-ns 2>/dev/null || echo "  ❌ S3 namespace not found"
	@echo ""
	@echo "Monitoring:"
	kubectl get pods -n monitoring 2>/dev/null || echo "  ❌ Monitoring namespace not found"
	@echo ""
	@echo "Services:"
	kubectl get svc --all-namespaces | grep -E "(sftp|s3|grafana|prometheus)" || echo "  ❌ No services found"

run-streamer: install
	@echo "🚀 Starting SFTP to S3 streaming application..."
	@echo "📋 Make sure you have configured your .env file with:"
	@echo "   SFTP_HOST=localhost"
	@echo "   SFTP_PORT=30022"
	@echo "   SFTP_USER=airflow"
	@echo "   SFTP_PASSWORD=airflow123"
	@echo "   S3_BUCKET=data"
	@echo "   AWS_ACCESS_KEY_ID=your_access_key"
	@echo "   AWS_SECRET_ACCESS_KEY=your_secret_key"
	@echo "   PUSH_GATEWAY=http://localhost:9091"
	@echo ""
	poetry run python run_streamer.py

clean-all:
	@echo "🧹 Cleaning up all Kubernetes deployments..."
	kubectl delete namespace sftp-ns --ignore-not-found=true
	kubectl delete namespace s3-ns --ignore-not-found=true  
	kubectl delete namespace monitoring --ignore-not-found=true
	@echo "✅ All deployments cleaned up"

dev-setup: sftp-server s3-server monitoring
	@echo "🛠️  Development environment setup complete!"
	@echo ""
	@echo "Next steps:"
	@echo "1. Configure your .env file"
	@echo "2. Run: make run-streamer"
	@echo "3. Monitor at: http://localhost:3000"

generate-test-file:
	@echo "📄 Generating test file for upload..."
	poetry run python scripts/generate_file.py