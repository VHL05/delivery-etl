import pandas as pd
import json
import time
import uuid
from datetime import datetime
from confluent_kafka import Producer



# Kafka event generator
# Mục tiêu: tạo ra các bản tin sự kiện giao hàng từ dữ liệu trong file CSV và gửi chúng đến một Kafka Topic.
# input: orders_clean.csv 
# output: các bản tin json được gửi liên tục đến Kafka Topic (delivery_events)

# đọc csv -> lặp từng order bóc tách các thông tin trạng thái (tạo đơn, duyệt đơn, hủy đơn, giao hàng) ứng với từng trạng thái của đơn hàng -> tạo ra các bản tin json -> gửi đến Kafka Topic (delivery_events) theo khóa order_id


## HÀM HỖ TRỢ
## đóng gói sự kiện đơn lẻ thành cấu trúc json với 5 thông tin: event_id, order_id, event_type, event_timestamp, produced_at

def create_event(order_id, event_type, event_timestamp):
    return {
        "event_id": str(uuid.uuid4()), # tạo mã ngâu nhiên cho event
        "order_id": str(order_id), # order_id từ dữ liệu csv
        "event_type": event_type, # loại sự kiện (tạo đơn, duyệt đơn, hủy đơn, giao hàng)
        "event_timestamp": str(event_timestamp), # thời gian sự kiện (từ dữ liệu csv)
        "produced_at": datetime.utcnow().isoformat() + "Z"   # thời gian tạo sự kiện (thời gian hiện tại)
    }


## HÀM CHÍNH

def main():
    # B1: Đọc dữ liệu từ CSV và sort theo thời gian
    df = pd.read_csv('data/processed_local/orders_clean.csv')
    # chuyển đổi thời gian mua hàng qua datetime
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    # sắp xếp tăng dần theo thời gian mua hàng
    df = df.sort_values('order_purchase_timestamp').reset_index(drop = True)


    # B2: Khởi tạo Kafka Producer
    # Cấu hình địa chỉ kết nối tới Kafka Local qua Docker (9092)
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'socket.timeout.ms': 5000,
        'queue.buffering.max.messages': 10000
    }
    producer = Producer(conf)

    topic_name = 'delivery_events'
    print(f"Producing events to topic: {topic_name}")
    event_count = 0

    # B3: Lặp qua từng order trong DataFrame
    for index, row in df.iterrows():
        order_id = row['order_id']
        events_to_send = []

        # Kiểm tra: Nếu mốc thời gian tồn tại (k0 null) thì tạo event tương ứng
        if pd.notnull(row['order_purchase_timestamp']):
            events_to_send.append(create_event(order_id, 'ORDER_CREATED', row['order_purchase_timestamp']))
            
        if pd.notnull(row['order_approved_at']):
            events_to_send.append(create_event(order_id, 'ORDER_APPROVED', row['order_approved_at']))
            
        if pd.notnull(row['order_delivered_carrier_date']):
            events_to_send.append(create_event(order_id, 'SHIPPED', row['order_delivered_carrier_date']))
            
        if pd.notnull(row['order_delivered_customer_date']):
            events_to_send.append(create_event(order_id, 'DELIVERED', row['order_delivered_customer_date']))

        # B4: Gửi các sự kiện đến Kafka Topic
        # lần lượt gửi từng sự kiện của đơn hàng này lên Kafka
        for event in events_to_send:
            # chuyển qua json dạng bytes để Kafka đọc đc
            payload = json.dumps(event).encode('utf-8')
            # gán key = order_id để toàn bộ event của 1 đơn hàng chui vào chung 1 partition
            producer.produce(
                topic = topic_name,
                key = order_id.encode('utf-8'),
                value = payload
            )
            event_count += 1

        # kích hoạt callback của Kafka
        producer.poll(0)

        # giả lập độ trễ thời gian thực giữa các đơn hàng
        time.sleep(0.01)  

        if (index + 1) % 200 == 0:
            print(f"-> Đã xử lý: {index + 1} đơn hàng | Tổng số event đã gửi: {event_count}")

    producer.flush()  # đảm bảo tất cả các sự kiện đã được gửi trước khi kết thúc
    print(f"Finished producing events. Total events sent: {event_count}")

if __name__ == "__main__":
    main()