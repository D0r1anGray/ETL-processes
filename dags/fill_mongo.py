from pymongo import MongoClient
from datetime import datetime, timedelta
import random


def fill_mongo():
    client = MongoClient("mongodb://root:example@mongodb:27017/")
    db = client["education_db"]

    db.user_sessions.drop()
    db.support_tickets.drop()

    sessions = [
        {
            "session_id": f"sess_{i:03d}",
            "user_id": f"user_{random.randint(100, 150)}",
            "start_time": datetime.now() - timedelta(hours=random.randint(1, 10)),
            "end_time": datetime.now(),
            "pages_visited": ["/home", "/products", "/cart", "/checkout"],
            "device": random.choice(["mobile", "desktop"]),
            "actions": ["login", "view_product", "add_to_cart"]
        } for i in range(1, 11)
    ]
    db.user_sessions.insert_many(sessions)

    tickets = [
        {
            "ticket_id": f"ticket_{i:03d}",
            "user_id": f"user_{random.randint(100, 150)}",
            "status": random.choice(["open", "closed", "pending"]),
            "issue_type": random.choice(["payment", "technical", "delivery"]),
            "messages": [
                {"sender": "user", "message": "Проблема с оплатой", "timestamp": datetime.now()}
            ],
            "created_at": datetime.now() - timedelta(days=1),
            "updated_at": datetime.now()
        } for i in range(1, 6)
    ]
    db.support_tickets.insert_many(tickets)

    print("Данные успешно загружены в MongoDB!")


if __name__ == "__main__":
    fill_mongo()