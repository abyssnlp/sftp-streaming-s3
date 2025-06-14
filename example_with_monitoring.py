import os
import asyncio
from sftp_streaming.main import AsyncSFTPToS3Streamer


async def main():
    ssh_params = {
        "host": os.getenv("SFTP_HOST", "localhost"),
        "username": os.getenv("SFTP_USER", "airflow"),
        "password": os.getenv("SFTP_PASSWORD", "airflow123"),
        "port": int(os.getenv("SFTP_PORT", "30022")),
    }

    streamer = AsyncSFTPToS3Streamer(
        bucket_name=os.getenv("S3_BUCKET", "data"),
        push_gateway_url=os.getenv("PUSH_GATEWAY", "http://localhost:9091"),
        job_name="async-concurrent-file-transfer",
        ssh_connection_params=ssh_params,
        s3_endpoint_url="http://localhost:9000",
    )

    file_path = "/upload/customers_10gb.csv"

    print(f"🚀 Starting HIGH-PERFORMANCE ASYNC CONCURRENT transfer of {file_path}")
    print("💡 Features enabled:")
    print("   • 4 concurrent async SFTP readers")
    print("   • 8 concurrent async S3 uploaders")
    print("   • Async connection pooling")
    print("   • Real-time performance monitoring")
    print("   • Expected significant speed improvement with asyncio!")
    print()
    print("📊 Monitor real-time performance:")
    print("   • Prometheus metrics pushed every 2 seconds")
    print("   • Check updated Grafana dashboard for concurrency analytics")
    print("   • Watch for improved throughput with async operations!")

    try:
        await streamer.upload_to_s3(file_path)
        print("\n🎉 ASYNC CONCURRENT TRANSFER COMPLETED SUCCESSFULLY! 🎉")
        print("📈 Check Grafana dashboard to see the async performance gains!")

    except Exception as e:
        print(f"❌ Transfer failed: {e}")
        print(
            "💡 If you see SSH connection errors, ensure all SSH connection details are correct"
        )


if __name__ == "__main__":
    asyncio.run(main())
