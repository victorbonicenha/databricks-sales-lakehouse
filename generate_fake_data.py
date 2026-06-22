from __future__ import annotations
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"

random.seed(42)

CUSTOMERS_DIR = RAW_DIR / "customers"
PRODUCTS_DIR = RAW_DIR / "products"
ORDERS_DIR = RAW_DIR / "orders"
PAYMENTS_DIR = RAW_DIR / "payments"

for folder in [CUSTOMERS_DIR, PRODUCTS_DIR, ORDERS_DIR, PAYMENTS_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False) + "\n")


first_names = [
    "Ana", "Bruno", "Carla", "Diego", "Eduarda", "Felipe", "Gabriela",
    "Henrique", "Isabela", "Joao", "Larissa", "Marcos", "Natalia", "Otavio",
    "Paula", "Rafael", "Sofia", "Thiago", "Vanessa", "Victor"
]
last_names = [
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Almeida",
    "Costa", "Gomes", "Ribeiro"
]
countries = ["BR", "BR", "BR", "AR", "CL", "US"]

customers = []
for customer_id in range(1, 51):
    name = f"{random.choice(first_names)} {random.choice(last_names)}"
    email_name = name.lower().replace(" ", ".")
    customers.append(
        {
            "customer_id": customer_id,
            "customer_name": name,
            "email": f"{email_name}{customer_id}@email.com",
            "country": random.choice(countries),
            "signup_date": (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 450))).date().isoformat(),
        }
    )

# Duplicado proposital
customers.append(customers[4].copy())
# Registro com email nulo proposital
customers.append({"customer_id": 999, "customer_name": "Cliente Sem Email", "email": "", "country": "BR", "signup_date": "2026-02-10"})

products = [
    {"product_id": 1, "product_name": "Notebook Basic", "category": "Electronics", "unit_price": 3200.00},
    {"product_id": 2, "product_name": "Wireless Mouse", "category": "Electronics", "unit_price": 89.90},
    {"product_id": 3, "product_name": "Mechanical Keyboard", "category": "Electronics", "unit_price": 299.90},
    {"product_id": 4, "product_name": "Office Chair", "category": "Furniture", "unit_price": 799.00},
    {"product_id": 5, "product_name": "Standing Desk", "category": "Furniture", "unit_price": 1299.00},
    {"product_id": 6, "product_name": "Data Engineering Book", "category": "Books", "unit_price": 159.90},
    {"product_id": 7, "product_name": "SQL Pocket Guide", "category": "Books", "unit_price": 79.90},
    {"product_id": 8, "product_name": "Gaming Headset", "category": "Electronics", "unit_price": 399.90},
    {"product_id": 9, "product_name": "Monitor 27", "category": "Electronics", "unit_price": 1499.00},
    {"product_id": 10, "product_name": "Coffee Mug", "category": "Office", "unit_price": 49.90},
    # Produto inválido proposital
    {"product_id": 11, "product_name": "Invalid Product", "category": "Testing", "unit_price": -10.00},
]

order_statuses = ["completed", "completed", "completed", "cancelled", "pending", "shipped", "COMPLETED", "Canceled"]
payment_methods = ["credit_card", "pix", "boleto", "debit_card"]
payment_statuses = ["approved", "approved", "approved", "pending", "refused"]

orders = []
payments = []
start_date = datetime(2026, 1, 1)

for order_id in range(1001, 1151):
    product = random.choice(products[:-1])
    quantity = random.randint(1, 5)
    order_date = start_date + timedelta(days=random.randint(0, 120))
    unit_price = float(product["unit_price"])
    status = random.choice(order_statuses)

    customer_id = random.randint(1, 50)
    if random.random() < 0.03:
        customer_id = 8888  # cliente inexistente proposital

    if random.random() < 0.02:
        quantity = -1  # quantidade inválida proposital

    orders.append(
        {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product["product_id"],
            "order_date": order_date.date().isoformat(),
            "status": status,
            "quantity": quantity,
            "unit_price": round(unit_price, 2),
        }
    )

    amount = round(quantity * unit_price, 2)
    if random.random() < 0.04:
        amount = round(amount + random.choice([10, -15, 25]), 2)  # divergência proposital

    payments.append(
        {
            "payment_id": f"PAY-{order_id}",
            "order_id": order_id,
            "payment_method": random.choice(payment_methods),
            "payment_status": random.choice(payment_statuses),
            "amount": amount,
            "paid_at": (order_date + timedelta(hours=random.randint(1, 72))).isoformat(),
        }
    )

# Pedido duplicado proposital
orders.append(orders[10].copy())

# Pedido com data inválida proposital
orders.append(
    {
        "order_id": 9999,
        "customer_id": 1,
        "product_id": 1,
        "order_date": "not-a-date",
        "status": "completed",
        "quantity": 1,
        "unit_price": 3200.0,
    }
)

write_csv(CUSTOMERS_DIR / "customers.csv", customers)
write_csv(PRODUCTS_DIR / "products.csv", products)
write_csv(ORDERS_DIR / "orders_2026_q1.csv", orders[:80])
write_csv(ORDERS_DIR / "orders_2026_q2.csv", orders[80:])
write_jsonl(PAYMENTS_DIR / "payments_2026.json", payments)

print(f"Dados gerados em: {RAW_DIR}")
