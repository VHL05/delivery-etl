import os
import json
import time
import uuid
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaError


# 1. CẤU HÌNH AWS
# Load biến môi trường từ file .env
load_dotenv()

AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Tạo S3 client
s3_client = boto3.client(
    's3',
    region_name=AWS_REGION
)


# 2. HÀM KIỂM TRA EVENT

def validate_event(event_dict):
    """
    Kiểm tra xem event có đủ 5 trường dữ liệu bắt buộc hay không.
    Tránh đưa dữ liệu không hợp lệ vào S3.
    """

    required_keys = [
        'event_id',
        'order_id',
        'event_type',
        'event_timestamp',
        'produced_at'
    ]

    for key in required_keys:
        if key not in event_dict:
            return False

    return True


# 3. HÀM GHI BATCH DỮ LIỆU VÀO S3

def upload_to_s3(batch_data, batch_id):
    """
    Nhận một batch chứa các delivery events
    và upload lên S3.

    Tự động thử lại tối đa 3 lần nếu upload thất bại.
    """

    # Đường dẫn object trên S3
    file_name = f"raw/delivery_events/batch_{batch_id}.json"

    # Chuyển batch thành JSON Lines
    json_lines = "\n".join(
        [json.dumps(record) for record in batch_data]
    )

    # Số lần thử lại tối đa
    max_retries = 3

    for attempt in range(max_retries):

        try:

            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=file_name,
                Body=json_lines
            )

            print(
                f"Batch {batch_id} uploaded to S3 successfully."
            )

            return True

        except ClientError as e:

            print(
                f"Attempt {attempt + 1} failed: {e}"
            )

            # Chỉ sleep nếu vẫn còn lần retry
            if attempt < max_retries - 1:
                time.sleep(2)

    print(
        f"Failed to upload batch {batch_id} "
        f"to S3 after {max_retries} attempts."
    )

    return False


# 4. VÒNG LẶP ĐIỀU PHỐI CHÍNH

def main():

    # Cấu hình Kafka Consumer

    consumer_config = {
        'bootstrap.servers': 'localhost:9092',

        # Consumer group riêng cho việc ghi S3
        'group.id': 'delivery_s3_writer_group',

        # Đọc từ event cũ nhất nếu chưa có offset
        'auto.offset.reset': 'earliest',

        # Tắt auto commit
        'enable.auto.commit': False
    }

    # Tạo Kafka Consumer
    consumer = Consumer(consumer_config)

    # Subscribe topic delivery_events
    consumer.subscribe(['delivery_events'])

    print(
        "Kafka consumer started. "
        "Listening for messages..."
    )


    # Thiết lập Micro-batching

    # Upload khi batch đạt 500 events
    MAX_BATCH_SIZE = 500

    # Hoặc upload nếu đã chờ 10 giây
    MAX_WAIT_TIME = 10
    
    # Danh sách chứa events hiện tại
    batch_data = []

    # Thời điểm bắt đầu batch
    batch_start_time = time.time()

    # 5. MAIN CONSUMPTION LOOP

    try:

        while True:

            # Poll Kafka

            # Chờ tối đa 1 giây để nhận message
            msg = consumer.poll(timeout=1.0)

            # Kiểm tra message
            if msg is None:
                # Không có message mới
                pass
            elif msg.error():
                # Nếu Kafka báo lỗi
                if msg.error().code() != KafkaError._PARTITION_EOF:

                    print(
                        f"Kafka error: {msg.error()}"
                    )
            else:
                # Parse message thành JSON
                try:
                    event_dict = json.loads(
                        msg.value().decode('utf-8')
                    )
                    # Validate event
                    if validate_event(event_dict):
                        # Event hợp lệ → thêm vào batch
                        batch_data.append(event_dict)
                    else:
                        # Event không hợp lệ
                        print(
                            "Invalid event structure. Skipping."
                        )
                except json.JSONDecodeError:
                    # Message không phải JSON hợp lệ
                    print(
                        "Received invalid JSON message. Skipping."
                    )
            # Kiểm tra điều kiện đóng batch
            time_elapsed = time.time() - batch_start_time

            # Upload nếu:
            # 1. Batch đủ 500 events
            # HOẶC
            # 2. Đã chờ đủ 10 giây
            if (
                len(batch_data) >= MAX_BATCH_SIZE
                or time_elapsed >= MAX_WAIT_TIME
            ):
                # Chỉ upload nếu batch có dữ liệu
                if len(batch_data) > 0:
                    # Tạo ID cho batch
                    batch_id = str(uuid.uuid4())[:8]
                    print(
                        f"Preparing batch {batch_id} "
                        f"with {len(batch_data)} events..."
                    )
                    # Upload batch lên S3

                    upload_success = upload_to_s3(
                        batch_data,
                        batch_id
                    )
                    # Commit Kafka offset
                    if upload_success:

                        # S3 ghi thành công
                        # commit Kafka offset
                        consumer.commit()

                        print(
                            f"Batch {batch_id} "
                            f"committed to Kafka."
                        )
                        # 5.8. Reset batch
                        batch_data = []
                        batch_start_time = time.time()
                    else:
                        # S3 upload thất bại
                        # KHÔNG commit Kafka
                        print(
                            f"Batch {batch_id} failed to upload. "
                            f"Not committing offsets."
                        )
                        # Không reset batch
                        # để giữ lại dữ liệu
                else:
                    # Không có dữ liệu trong batch
                    # chỉ reset timer
                    batch_start_time = time.time()
    # 6. XỬ LÝ KHI USER DỪNG PROGRAM
    except KeyboardInterrupt:
        print(
            "Consumer interrupted. Exiting..."
        )
    # 7. ĐÓNG KAFKA CONSUMER
    finally:
        consumer.close()
        print(
            "Kafka consumer closed."
        )
# 8. ENTRY POINT
if __name__ == "__main__":
    main()