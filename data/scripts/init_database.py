#!/usr/bin/env python3
"""
Database Initialization Script

Generates realistic sample data for SQL exercises using Faker.
Creates a single DuckDB database with all tables populated.

Usage:
    python init_database.py

This will create/recreate: data/databases/practice.duckdb
"""

import duckdb
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

# Initialize Faker with seed for reproducibility
fake = Faker()
Faker.seed(42)
random.seed(42)

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "databases" / "practice.duckdb"
SCHEMA_PATH = SCRIPT_DIR / "schema.sql"


def create_database():
    """Create fresh database with schema."""
    # Remove existing database
    if DB_PATH.exists():
        DB_PATH.unlink()

    # Create directories if needed
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect and create tables directly
    conn = duckdb.connect(str(DB_PATH))

    # Create all tables inline to avoid SQL parsing issues
    conn.execute("""
        -- Employees tables
        CREATE TABLE departments (
            department_id INTEGER PRIMARY KEY,
            department_name VARCHAR(100) NOT NULL,
            location VARCHAR(100),
            budget DECIMAL(15, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE employees (
            employee_id INTEGER PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE,
            phone VARCHAR(20),
            hire_date DATE NOT NULL,
            job_title VARCHAR(100),
            salary DECIMAL(10, 2),
            commission_pct DECIMAL(4, 2),
            manager_id INTEGER,
            department_id INTEGER,
            is_active BOOLEAN DEFAULT TRUE
        );

        CREATE TABLE salary_history (
            history_id INTEGER PRIMARY KEY,
            employee_id INTEGER,
            old_salary DECIMAL(10, 2),
            new_salary DECIMAL(10, 2),
            change_date DATE NOT NULL,
            change_reason VARCHAR(200)
        );

        CREATE TABLE projects (
            project_id INTEGER PRIMARY KEY,
            project_name VARCHAR(200) NOT NULL,
            start_date DATE,
            end_date DATE,
            budget DECIMAL(15, 2),
            status VARCHAR(20)
        );

        CREATE TABLE project_assignments (
            assignment_id INTEGER PRIMARY KEY,
            employee_id INTEGER,
            project_id INTEGER,
            role VARCHAR(50),
            hours_allocated INTEGER,
            start_date DATE,
            end_date DATE
        );

        CREATE TABLE performance_reviews (
            review_id INTEGER PRIMARY KEY,
            employee_id INTEGER,
            reviewer_id INTEGER,
            review_date DATE NOT NULL,
            rating INTEGER,
            comments TEXT
        );

        -- Ecommerce tables
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            phone VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP,
            customer_tier VARCHAR(20)
        );

        CREATE TABLE addresses (
            address_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            address_type VARCHAR(20),
            street_address VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(100),
            postal_code VARCHAR(20),
            country VARCHAR(100) DEFAULT 'USA',
            is_default BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE categories (
            category_id INTEGER PRIMARY KEY,
            category_name VARCHAR(100) NOT NULL,
            parent_category_id INTEGER,
            description TEXT
        );

        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            sku VARCHAR(50) UNIQUE NOT NULL,
            product_name VARCHAR(255) NOT NULL,
            description TEXT,
            category_id INTEGER,
            unit_price DECIMAL(10, 2) NOT NULL,
            cost_price DECIMAL(10, 2),
            stock_quantity INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_date TIMESTAMP NOT NULL,
            shipping_address_id INTEGER,
            billing_address_id INTEGER,
            order_status VARCHAR(20),
            subtotal DECIMAL(12, 2),
            tax_amount DECIMAL(10, 2),
            shipping_cost DECIMAL(10, 2),
            discount_amount DECIMAL(10, 2) DEFAULT 0,
            total_amount DECIMAL(12, 2),
            payment_method VARCHAR(50),
            notes TEXT
        );

        CREATE TABLE order_items (
            item_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(10, 2) NOT NULL,
            discount_pct DECIMAL(5, 2) DEFAULT 0,
            line_total DECIMAL(12, 2)
        );

        CREATE TABLE reviews (
            review_id INTEGER PRIMARY KEY,
            product_id INTEGER,
            customer_id INTEGER,
            rating INTEGER,
            review_title VARCHAR(200),
            review_text TEXT,
            review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_verified_purchase BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE promotions (
            promotion_id INTEGER PRIMARY KEY,
            promo_code VARCHAR(50) UNIQUE,
            description VARCHAR(255),
            discount_type VARCHAR(20),
            discount_value DECIMAL(10, 2),
            min_order_amount DECIMAL(10, 2),
            start_date DATE,
            end_date DATE,
            usage_limit INTEGER,
            times_used INTEGER DEFAULT 0
        );

        -- Analytics tables
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            anonymous_id VARCHAR(100),
            email VARCHAR(255),
            signup_date DATE,
            signup_source VARCHAR(50),
            country VARCHAR(100),
            device_type VARCHAR(20),
            is_premium BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE sessions (
            session_id VARCHAR(100) PRIMARY KEY,
            user_id INTEGER,
            session_start TIMESTAMP NOT NULL,
            session_end TIMESTAMP,
            landing_page VARCHAR(500),
            exit_page VARCHAR(500),
            device_type VARCHAR(50),
            browser VARCHAR(50),
            os VARCHAR(50),
            referrer_source VARCHAR(100),
            referrer_medium VARCHAR(50),
            utm_campaign VARCHAR(100),
            page_views INTEGER DEFAULT 0,
            session_duration_seconds INTEGER
        );

        CREATE TABLE page_views (
            view_id INTEGER PRIMARY KEY,
            session_id VARCHAR(100),
            user_id INTEGER,
            page_url VARCHAR(500) NOT NULL,
            page_title VARCHAR(255),
            view_timestamp TIMESTAMP NOT NULL,
            time_on_page_seconds INTEGER,
            scroll_depth_pct INTEGER
        );

        CREATE TABLE events (
            event_id INTEGER PRIMARY KEY,
            session_id VARCHAR(100),
            user_id INTEGER,
            event_name VARCHAR(100) NOT NULL,
            event_category VARCHAR(100),
            event_timestamp TIMESTAMP NOT NULL,
            event_properties VARCHAR(1000),
            page_url VARCHAR(500)
        );

        CREATE TABLE conversions (
            conversion_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            session_id VARCHAR(100),
            conversion_type VARCHAR(50) NOT NULL,
            conversion_value DECIMAL(12, 2),
            conversion_timestamp TIMESTAMP NOT NULL,
            attribution_channel VARCHAR(100)
        );

        CREATE TABLE ab_tests (
            test_id INTEGER PRIMARY KEY,
            test_name VARCHAR(200) NOT NULL,
            start_date DATE,
            end_date DATE,
            status VARCHAR(20)
        );

        CREATE TABLE ab_test_assignments (
            assignment_id INTEGER PRIMARY KEY,
            test_id INTEGER,
            user_id INTEGER,
            variant VARCHAR(50) NOT NULL,
            assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE daily_metrics (
            metric_date DATE NOT NULL,
            metric_name VARCHAR(100) NOT NULL,
            metric_value DECIMAL(15, 4),
            segment VARCHAR(100) NOT NULL,
            PRIMARY KEY (metric_date, metric_name, segment)
        );
    """)

    return conn


# ============================================================
# EMPLOYEES DATA GENERATION
# ============================================================

DEPARTMENTS = [
    ("Engineering", "San Francisco", 2500000),
    ("Sales", "New York", 1800000),
    ("Marketing", "Los Angeles", 1200000),
    ("Human Resources", "Chicago", 800000),
    ("Finance", "Boston", 1500000),
    ("Operations", "Seattle", 1100000),
    ("Customer Support", "Austin", 900000),
    ("Research", "San Francisco", 2000000),
]

JOB_TITLES = {
    "Engineering": ["Software Engineer", "Senior Software Engineer", "Staff Engineer",
                   "Engineering Manager", "DevOps Engineer", "QA Engineer"],
    "Sales": ["Sales Representative", "Senior Sales Rep", "Sales Manager",
              "Account Executive", "Sales Director"],
    "Marketing": ["Marketing Coordinator", "Marketing Manager", "Content Strategist",
                 "Brand Manager", "Marketing Director"],
    "Human Resources": ["HR Coordinator", "HR Manager", "Recruiter",
                       "HR Business Partner", "HR Director"],
    "Finance": ["Financial Analyst", "Senior Accountant", "Finance Manager",
               "Controller", "CFO"],
    "Operations": ["Operations Coordinator", "Operations Manager", "Supply Chain Analyst",
                  "Logistics Manager", "COO"],
    "Customer Support": ["Support Representative", "Support Specialist", "Support Manager",
                        "Customer Success Manager"],
    "Research": ["Research Scientist", "Senior Researcher", "Research Director",
                "Data Scientist", "ML Engineer"],
}

SALARY_RANGES = {
    "Coordinator": (45000, 65000),
    "Representative": (50000, 75000),
    "Analyst": (60000, 90000),
    "Engineer": (80000, 140000),
    "Specialist": (55000, 85000),
    "Manager": (90000, 150000),
    "Senior": (100000, 160000),
    "Staff": (140000, 200000),
    "Director": (150000, 220000),
    "Scientist": (90000, 160000),
}


def get_salary_for_title(title):
    """Get appropriate salary range based on job title."""
    for keyword, (low, high) in SALARY_RANGES.items():
        if keyword.lower() in title.lower():
            return random.randint(low, high)
    return random.randint(50000, 100000)


def generate_employees_data(conn):
    """Generate employees-related data."""
    print("Generating employees data...")

    # Insert departments
    for i, (name, location, budget) in enumerate(DEPARTMENTS, 1):
        conn.execute("""
            INSERT INTO departments (department_id, department_name, location, budget)
            VALUES (?, ?, ?, ?)
        """, [i, name, location, budget])

    # Generate employees
    employees = []
    employee_id = 1

    # First pass: create all employees (managers first)
    for dept_id, (dept_name, _, _) in enumerate(DEPARTMENTS, 1):
        titles = JOB_TITLES.get(dept_name, ["Staff"])

        # Department head (no manager initially)
        manager_title = [t for t in titles if "Director" in t or "Manager" in t]
        if manager_title:
            head_title = manager_title[-1]  # Highest level
        else:
            head_title = titles[-1]

        # Create department head
        emp = {
            "id": employee_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "hire_date": fake.date_between(start_date="-10y", end_date="-5y"),
            "job_title": head_title,
            "salary": get_salary_for_title(head_title),
            "commission_pct": 0.15 if "Sales" in dept_name else None,
            "manager_id": None,  # Department heads report to CEO (NULL)
            "department_id": dept_id,
        }
        emp["email"] = f"{emp['first_name'].lower()}.{emp['last_name'].lower()}@company.com"
        employees.append(emp)
        dept_head_id = employee_id
        employee_id += 1

        # Create regular employees in department (15-20 per dept)
        num_employees = random.randint(15, 22)
        for _ in range(num_employees):
            title = random.choice(titles)
            emp = {
                "id": employee_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "hire_date": fake.date_between(start_date="-8y", end_date="-1m"),
                "job_title": title,
                "salary": get_salary_for_title(title),
                "commission_pct": round(random.uniform(0.05, 0.20), 2) if "Sales" in dept_name else None,
                "manager_id": dept_head_id,
                "department_id": dept_id,
            }
            emp["email"] = f"{emp['first_name'].lower()}.{emp['last_name'].lower()}{employee_id}@company.com"
            employees.append(emp)
            employee_id += 1

    # Insert employees
    for emp in employees:
        conn.execute("""
            INSERT INTO employees (employee_id, first_name, last_name, email, phone,
                                  hire_date, job_title, salary, commission_pct,
                                  manager_id, department_id, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            emp["id"], emp["first_name"], emp["last_name"], emp["email"],
            fake.phone_number()[:20], emp["hire_date"], emp["job_title"],
            emp["salary"], emp["commission_pct"], emp["manager_id"],
            emp["department_id"], random.random() > 0.05  # 95% active
        ])

    # Generate salary history
    print("Generating salary history...")
    history_id = 1
    for emp in employees:
        # Each employee has 0-4 salary changes
        num_changes = random.randint(0, 4)
        current_salary = emp["salary"]
        change_date = emp["hire_date"]

        for _ in range(num_changes):
            old_salary = int(current_salary * random.uniform(0.85, 0.95))
            change_date = fake.date_between(start_date=change_date, end_date="today")
            reason = random.choice(["Annual raise", "Promotion", "Market adjustment",
                                   "Performance bonus", "Role change"])

            conn.execute("""
                INSERT INTO salary_history (history_id, employee_id, old_salary,
                                           new_salary, change_date, change_reason)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [history_id, emp["id"], old_salary, current_salary, change_date, reason])

            current_salary = old_salary
            history_id += 1

    # Generate projects
    print("Generating projects...")
    PROJECT_NAMES = [
        "Website Redesign", "Mobile App Launch", "Data Migration",
        "CRM Integration", "Security Audit", "Cloud Migration",
        "API Development", "Analytics Dashboard", "Inventory System",
        "Customer Portal", "Payment Gateway", "Search Optimization",
        "Performance Tuning", "Documentation Update", "Training Program",
        "Market Research", "Brand Refresh", "Product Launch Q1",
        "Product Launch Q2", "Infrastructure Upgrade", "DevOps Pipeline",
        "Testing Framework", "Compliance Review", "Budget Planning",
        "Annual Report"
    ]

    for proj_id, name in enumerate(PROJECT_NAMES, 1):
        start = fake.date_between(start_date="-2y", end_date="today")
        end_date = start + timedelta(days=random.randint(30, 365))
        status = random.choice(["planning", "active", "completed", "completed", "completed"])
        if end_date > datetime.now().date():
            status = random.choice(["planning", "active"])

        conn.execute("""
            INSERT INTO projects (project_id, project_name, start_date, end_date, budget, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [proj_id, name, start, end_date, random.randint(50000, 500000), status])

    # Generate project assignments
    print("Generating project assignments...")
    assignment_id = 1
    for proj_id in range(1, len(PROJECT_NAMES) + 1):
        # Assign 3-8 employees per project
        num_assigned = random.randint(3, 8)
        assigned_emps = random.sample(range(1, len(employees) + 1), num_assigned)

        for emp_id in assigned_emps:
            role = random.choice(["Lead", "Developer", "Analyst", "Reviewer", "Contributor"])
            conn.execute("""
                INSERT INTO project_assignments (assignment_id, employee_id, project_id,
                                                role, hours_allocated, start_date, end_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                assignment_id, emp_id, proj_id, role,
                random.randint(20, 200),
                fake.date_between(start_date="-1y", end_date="today"),
                fake.date_between(start_date="today", end_date="+6m") if random.random() > 0.3 else None
            ])
            assignment_id += 1

    # Generate performance reviews
    print("Generating performance reviews...")
    review_id = 1
    for emp in employees:
        # 1-3 reviews per employee
        num_reviews = random.randint(1, 3)
        for _ in range(num_reviews):
            # Reviewer is the employee's manager or random senior employee
            reviewer_id = emp["manager_id"] if emp["manager_id"] else random.randint(1, 8)

            conn.execute("""
                INSERT INTO performance_reviews (review_id, employee_id, reviewer_id,
                                                review_date, rating, comments)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                review_id, emp["id"], reviewer_id,
                fake.date_between(start_date="-2y", end_date="today"),
                random.choices([1, 2, 3, 4, 5], weights=[1, 5, 20, 50, 24])[0],
                fake.paragraph(nb_sentences=3)
            ])
            review_id += 1

    print(f"  Created {len(employees)} employees")


# ============================================================
# ECOMMERCE DATA GENERATION
# ============================================================

PRODUCT_CATEGORIES = [
    (1, "Electronics", None),
    (2, "Computers", 1),
    (3, "Phones", 1),
    (4, "Audio", 1),
    (5, "Clothing", None),
    (6, "Men's Clothing", 5),
    (7, "Women's Clothing", 5),
    (8, "Home & Garden", None),
    (9, "Kitchen", 8),
    (10, "Furniture", 8),
    (11, "Sports", None),
    (12, "Fitness", 11),
    (13, "Outdoor", 11),
    (14, "Books", None),
    (15, "Toys", None),
]

PRODUCTS = [
    # Electronics > Computers
    ("Laptop Pro 15", 2, 1299.99, 899.00),
    ("Desktop Workstation", 2, 1899.99, 1299.00),
    ("Gaming Laptop", 2, 1599.99, 1099.00),
    ("Ultrabook Air", 2, 999.99, 699.00),
    ("Mini PC", 2, 499.99, 349.00),
    # Electronics > Phones
    ("Smartphone X", 3, 899.99, 599.00),
    ("Smartphone Lite", 3, 499.99, 299.00),
    ("Flip Phone Pro", 3, 1199.99, 799.00),
    ("Budget Phone", 3, 199.99, 99.00),
    # Electronics > Audio
    ("Wireless Earbuds", 4, 149.99, 79.00),
    ("Over-ear Headphones", 4, 299.99, 159.00),
    ("Bluetooth Speaker", 4, 79.99, 39.00),
    ("Soundbar", 4, 399.99, 219.00),
    # Clothing > Men's
    ("Men's T-Shirt", 6, 29.99, 12.00),
    ("Men's Jeans", 6, 59.99, 29.00),
    ("Men's Jacket", 6, 129.99, 69.00),
    ("Men's Sneakers", 6, 89.99, 45.00),
    # Clothing > Women's
    ("Women's Dress", 7, 79.99, 39.00),
    ("Women's Blouse", 7, 49.99, 24.00),
    ("Women's Jeans", 7, 69.99, 34.00),
    ("Women's Boots", 7, 119.99, 59.00),
    # Home > Kitchen
    ("Coffee Maker", 9, 89.99, 49.00),
    ("Blender Pro", 9, 129.99, 69.00),
    ("Toaster Oven", 9, 79.99, 39.00),
    ("Knife Set", 9, 149.99, 79.00),
    # Home > Furniture
    ("Office Chair", 10, 299.99, 149.00),
    ("Standing Desk", 10, 499.99, 279.00),
    ("Bookshelf", 10, 179.99, 89.00),
    ("Sofa", 10, 899.99, 499.00),
    # Sports > Fitness
    ("Yoga Mat", 12, 39.99, 19.00),
    ("Dumbbells Set", 12, 149.99, 79.00),
    ("Exercise Bike", 12, 399.99, 219.00),
    ("Resistance Bands", 12, 24.99, 12.00),
    # Sports > Outdoor
    ("Camping Tent", 13, 199.99, 109.00),
    ("Hiking Backpack", 13, 89.99, 49.00),
    ("Sleeping Bag", 13, 79.99, 39.00),
    ("Trekking Poles", 13, 59.99, 29.00),
    # Books
    ("Programming Guide", 14, 49.99, 24.00),
    ("Business Strategy", 14, 29.99, 14.00),
    ("Science Fiction Novel", 14, 14.99, 7.00),
    ("Cookbook", 14, 34.99, 17.00),
    # Toys
    ("Building Blocks Set", 15, 49.99, 24.00),
    ("Board Game Collection", 15, 39.99, 19.00),
    ("Remote Control Car", 15, 79.99, 39.00),
    ("Puzzle 1000 Pieces", 15, 19.99, 9.00),
]


def generate_ecommerce_data(conn):
    """Generate ecommerce-related data."""
    print("Generating ecommerce data...")

    # Insert categories
    for cat_id, name, parent_id in PRODUCT_CATEGORIES:
        conn.execute("""
            INSERT INTO categories (category_id, category_name, parent_category_id, description)
            VALUES (?, ?, ?, ?)
        """, [cat_id, name, parent_id, fake.sentence()])

    # Insert products
    for prod_id, (name, cat_id, price, cost) in enumerate(PRODUCTS, 1):
        conn.execute("""
            INSERT INTO products (product_id, sku, product_name, description, category_id,
                                 unit_price, cost_price, stock_quantity, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            prod_id, f"SKU-{prod_id:05d}", name, fake.paragraph(),
            cat_id, price, cost, random.randint(0, 500),
            random.random() > 0.1  # 90% active
        ])

    # Generate customers
    print("Generating customers...")
    customers = []
    for cust_id in range(1, 501):
        created = fake.date_time_between(start_date="-3y", end_date="-1d")
        customer = {
            "id": cust_id,
            "email": fake.unique.email(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "phone": fake.phone_number()[:20],
            "created_at": created,
            "last_login": fake.date_time_between(start_date=created, end_date="now"),
            "tier": random.choices(
                ["bronze", "silver", "gold", "platinum"],
                weights=[50, 30, 15, 5]
            )[0]
        }
        customers.append(customer)

        conn.execute("""
            INSERT INTO customers (customer_id, email, first_name, last_name, phone,
                                  created_at, last_login, customer_tier)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            customer["id"], customer["email"], customer["first_name"],
            customer["last_name"], customer["phone"], customer["created_at"],
            customer["last_login"], customer["tier"]
        ])

    # Generate addresses
    print("Generating addresses...")
    address_id = 1
    for customer in customers:
        # Each customer has 1-3 addresses
        num_addresses = random.randint(1, 3)
        for i in range(num_addresses):
            addr_type = "shipping" if i == 0 else random.choice(["shipping", "billing"])
            conn.execute("""
                INSERT INTO addresses (address_id, customer_id, address_type, street_address,
                                      city, state, postal_code, country, is_default)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                address_id, customer["id"], addr_type,
                fake.street_address(), fake.city(), fake.state_abbr(),
                fake.zipcode(), "USA", i == 0
            ])
            address_id += 1

    # Generate orders
    print("Generating orders...")
    order_id = 1
    item_id = 1

    for _ in range(2000):
        customer = random.choice(customers)
        order_date = fake.date_time_between(start_date="-2y", end_date="now")

        # Get customer's addresses
        addresses = conn.execute("""
            SELECT address_id FROM addresses WHERE customer_id = ?
        """, [customer["id"]]).fetchall()

        if not addresses:
            continue

        shipping_addr = addresses[0][0]
        billing_addr = addresses[-1][0]

        # Generate order items first to calculate totals
        num_items = random.randint(1, 5)
        product_ids = random.sample(range(1, len(PRODUCTS) + 1), min(num_items, len(PRODUCTS)))

        items = []
        subtotal = 0
        for prod_id in product_ids:
            qty = random.randint(1, 3)
            price = PRODUCTS[prod_id - 1][2]  # unit_price
            discount = random.choice([0, 0, 0, 0.1, 0.15, 0.2])
            line_total = round(qty * price * (1 - discount), 2)
            items.append({
                "product_id": prod_id,
                "quantity": qty,
                "unit_price": price,
                "discount_pct": discount,
                "line_total": line_total
            })
            subtotal += line_total

        tax = round(subtotal * 0.08, 2)
        shipping = round(random.uniform(5, 15), 2) if subtotal < 100 else 0
        discount_amount = round(subtotal * random.choice([0, 0, 0, 0.05, 0.1]), 2)
        total = round(subtotal + tax + shipping - discount_amount, 2)

        status = random.choices(
            ["pending", "processing", "shipped", "delivered", "cancelled", "returned"],
            weights=[5, 10, 15, 60, 7, 3]
        )[0]

        conn.execute("""
            INSERT INTO orders (order_id, customer_id, order_date, shipping_address_id,
                               billing_address_id, order_status, subtotal, tax_amount,
                               shipping_cost, discount_amount, total_amount, payment_method)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            order_id, customer["id"], order_date, shipping_addr, billing_addr,
            status, subtotal, tax, shipping, discount_amount, total,
            random.choice(["credit_card", "debit_card", "paypal", "apple_pay"])
        ])

        # Insert order items
        for item in items:
            conn.execute("""
                INSERT INTO order_items (item_id, order_id, product_id, quantity,
                                        unit_price, discount_pct, line_total)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                item_id, order_id, item["product_id"], item["quantity"],
                item["unit_price"], item["discount_pct"], item["line_total"]
            ])
            item_id += 1

        order_id += 1

    # Generate reviews
    print("Generating reviews...")
    review_id = 1
    for _ in range(1000):
        customer = random.choice(customers)
        product_id = random.randint(1, len(PRODUCTS))

        conn.execute("""
            INSERT INTO reviews (review_id, product_id, customer_id, rating,
                                review_title, review_text, review_date, is_verified_purchase)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            review_id, product_id, customer["id"],
            random.choices([1, 2, 3, 4, 5], weights=[5, 10, 15, 35, 35])[0],
            fake.sentence(nb_words=6),
            fake.paragraph(nb_sentences=random.randint(1, 4)),
            fake.date_time_between(start_date="-1y", end_date="now"),
            random.random() > 0.3  # 70% verified
        ])
        review_id += 1

    # Generate promotions
    print("Generating promotions...")
    PROMO_CODES = [
        ("WELCOME10", "New customer discount", "percentage", 10),
        ("SAVE20", "20% off everything", "percentage", 20),
        ("FLAT15", "$15 off orders over $100", "fixed_amount", 15),
        ("FREESHIP", "Free shipping", "free_shipping", 0),
        ("SUMMER25", "Summer sale", "percentage", 25),
        ("HOLIDAY30", "Holiday special", "percentage", 30),
        ("VIP50", "VIP exclusive", "fixed_amount", 50),
        ("FLASH10", "Flash sale", "percentage", 10),
    ]

    for promo_id, (code, desc, dtype, value) in enumerate(PROMO_CODES, 1):
        start = fake.date_between(start_date="-6m", end_date="today")
        conn.execute("""
            INSERT INTO promotions (promotion_id, promo_code, description, discount_type,
                                   discount_value, min_order_amount, start_date, end_date,
                                   usage_limit, times_used)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            promo_id, code, desc, dtype, value,
            50 if dtype == "fixed_amount" else None,
            start, start + timedelta(days=random.randint(30, 180)),
            random.randint(100, 1000), random.randint(0, 200)
        ])

    print(f"  Created {len(customers)} customers, {order_id-1} orders")


# ============================================================
# ANALYTICS DATA GENERATION
# ============================================================

PAGES = [
    "/", "/products", "/products/category/electronics", "/products/category/clothing",
    "/products/123", "/products/456", "/products/789",
    "/cart", "/checkout", "/checkout/success",
    "/account", "/account/orders", "/account/settings",
    "/about", "/contact", "/blog", "/blog/post-1", "/blog/post-2",
    "/search", "/deals", "/new-arrivals"
]

EVENTS = [
    ("page_view", "engagement"),
    ("add_to_cart", "conversion"),
    ("remove_from_cart", "conversion"),
    ("begin_checkout", "conversion"),
    ("purchase", "conversion"),
    ("sign_up", "conversion"),
    ("login", "engagement"),
    ("search", "engagement"),
    ("product_view", "engagement"),
    ("share", "engagement"),
    ("newsletter_signup", "conversion"),
]

REFERRERS = [
    ("google", "organic"),
    ("google", "cpc"),
    ("facebook", "social"),
    ("instagram", "social"),
    ("twitter", "social"),
    ("direct", "none"),
    ("email", "email"),
    ("affiliate", "referral"),
]


def generate_analytics_data(conn):
    """Generate analytics-related data."""
    print("Generating analytics data...")

    # Generate users
    users = []
    for user_id in range(1, 1001):
        signup_date = fake.date_between(start_date="-2y", end_date="-1d")
        user = {
            "id": user_id,
            "anonymous_id": fake.uuid4(),
            "email": fake.email() if random.random() > 0.3 else None,
            "signup_date": signup_date,
            "signup_source": random.choice(["organic", "paid", "referral", "social", "direct"]),
            "country": random.choices(
                ["USA", "UK", "Canada", "Germany", "France", "Australia", "Japan", "Brazil"],
                weights=[40, 15, 10, 8, 7, 7, 7, 6]
            )[0],
            "device_type": random.choices(["desktop", "mobile", "tablet"], weights=[45, 45, 10])[0],
            "is_premium": random.random() > 0.85
        }
        users.append(user)

        conn.execute("""
            INSERT INTO users (user_id, anonymous_id, email, signup_date, signup_source,
                              country, device_type, is_premium)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            user["id"], user["anonymous_id"], user["email"], user["signup_date"],
            user["signup_source"], user["country"], user["device_type"], user["is_premium"]
        ])

    # Generate sessions
    print("Generating sessions...")
    sessions = []
    for _ in range(5000):
        user = random.choice(users)
        session_start = fake.date_time_between(start_date="-1y", end_date="now")
        duration = random.randint(30, 1800)  # 30 sec to 30 min
        session_end = session_start + timedelta(seconds=duration)
        referrer = random.choice(REFERRERS)

        session = {
            "id": fake.uuid4(),
            "user_id": user["id"],
            "start": session_start,
            "end": session_end,
            "duration": duration,
            "landing_page": random.choice(PAGES[:5]),
            "exit_page": random.choice(PAGES),
            "page_views": random.randint(1, 15),
        }
        sessions.append(session)

        conn.execute("""
            INSERT INTO sessions (session_id, user_id, session_start, session_end,
                                 landing_page, exit_page, device_type, browser, os,
                                 referrer_source, referrer_medium, utm_campaign,
                                 page_views, session_duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            session["id"], user["id"], session_start, session_end,
            session["landing_page"], session["exit_page"],
            random.choice(["desktop", "mobile", "tablet"]),
            random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
            random.choice(["Windows", "macOS", "iOS", "Android", "Linux"]),
            referrer[0], referrer[1],
            f"campaign_{random.randint(1, 10)}" if random.random() > 0.7 else None,
            session["page_views"], duration
        ])

    # Generate page views
    print("Generating page views...")
    view_id = 1
    for session in sessions:
        num_views = session["page_views"]
        current_time = session["start"]

        for _ in range(num_views):
            time_on_page = random.randint(5, 180)
            conn.execute("""
                INSERT INTO page_views (view_id, session_id, user_id, page_url, page_title,
                                       view_timestamp, time_on_page_seconds, scroll_depth_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                view_id, session["id"], session["user_id"],
                random.choice(PAGES),
                fake.sentence(nb_words=4),
                current_time,
                time_on_page,
                random.randint(10, 100)
            ])
            current_time += timedelta(seconds=time_on_page)
            view_id += 1

    # Generate events
    print("Generating events...")
    event_id = 1
    for session in sessions:
        # 1-10 events per session
        num_events = random.randint(1, 10)
        current_time = session["start"]

        for _ in range(num_events):
            event_name, event_category = random.choice(EVENTS)
            conn.execute("""
                INSERT INTO events (event_id, session_id, user_id, event_name, event_category,
                                   event_timestamp, event_properties, page_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                event_id, session["id"], session["user_id"],
                event_name, event_category,
                current_time,
                '{"value": ' + str(random.randint(1, 100)) + '}',
                random.choice(PAGES)
            ])
            current_time += timedelta(seconds=random.randint(5, 60))
            event_id += 1

    # Generate conversions
    print("Generating conversions...")
    conversion_id = 1
    for session in random.sample(sessions, min(800, len(sessions))):
        conn.execute("""
            INSERT INTO conversions (conversion_id, user_id, session_id, conversion_type,
                                    conversion_value, conversion_timestamp, attribution_channel)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            conversion_id, session["user_id"], session["id"],
            random.choice(["purchase", "signup", "subscription", "lead"]),
            round(random.uniform(10, 500), 2),
            session["start"] + timedelta(seconds=random.randint(1, max(2, session["duration"]))),
            random.choice(["organic", "paid_search", "social", "email", "direct", "referral"])
        ])
        conversion_id += 1

    # Generate A/B tests
    print("Generating A/B tests...")
    AB_TESTS = [
        ("Homepage Hero Test", "completed"),
        ("Checkout Button Color", "running"),
        ("Product Page Layout", "completed"),
        ("Pricing Display", "running"),
        ("Navigation Menu", "completed"),
    ]

    for test_id, (name, status) in enumerate(AB_TESTS, 1):
        start = fake.date_between(start_date="-6m", end_date="-1m")
        conn.execute("""
            INSERT INTO ab_tests (test_id, test_name, start_date, end_date, status)
            VALUES (?, ?, ?, ?, ?)
        """, [
            test_id, name, start,
            start + timedelta(days=random.randint(14, 60)) if status == "completed" else None,
            status
        ])

    # Generate A/B test assignments
    assignment_id = 1
    for user in random.sample(users, min(800, len(users))):
        test_id = random.randint(1, len(AB_TESTS))
        conn.execute("""
            INSERT INTO ab_test_assignments (assignment_id, test_id, user_id, variant, assigned_at)
            VALUES (?, ?, ?, ?, ?)
        """, [
            assignment_id, test_id, user["id"],
            random.choice(["control", "variant_a", "variant_b"]),
            fake.date_time_between(start_date="-6m", end_date="now")
        ])
        assignment_id += 1

    # Generate daily metrics
    print("Generating daily metrics...")
    METRICS = [
        "daily_active_users", "page_views", "sessions", "conversions",
        "revenue", "bounce_rate", "avg_session_duration", "new_users"
    ]
    SEGMENTS = ["all", "mobile", "desktop", "organic", "paid"]

    start_date = datetime.now().date() - timedelta(days=365)
    for day in range(365):
        metric_date = start_date + timedelta(days=day)
        for metric in METRICS:
            for segment in SEGMENTS:
                base_value = {
                    "daily_active_users": random.randint(100, 500),
                    "page_views": random.randint(500, 2000),
                    "sessions": random.randint(200, 800),
                    "conversions": random.randint(10, 100),
                    "revenue": round(random.uniform(500, 5000), 2),
                    "bounce_rate": round(random.uniform(0.3, 0.7), 4),
                    "avg_session_duration": random.randint(60, 300),
                    "new_users": random.randint(20, 100),
                }.get(metric, random.randint(10, 100))

                # Adjust for segment
                if segment != "all":
                    base_value = base_value * random.uniform(0.15, 0.35)

                conn.execute("""
                    INSERT INTO daily_metrics (metric_date, metric_name, metric_value, segment)
                    VALUES (?, ?, ?, ?)
                """, [metric_date, metric, base_value, segment])

    print(f"  Created {len(users)} users, {len(sessions)} sessions")


def main():
    """Main entry point."""
    print("=" * 60)
    print("SQL Exercises Database Initialization")
    print("=" * 60)

    print(f"\nCreating database at: {DB_PATH}")
    conn = create_database()

    print("\n" + "-" * 40)
    generate_employees_data(conn)

    print("\n" + "-" * 40)
    generate_ecommerce_data(conn)

    print("\n" + "-" * 40)
    generate_analytics_data(conn)

    conn.close()

    print("\n" + "=" * 60)
    print("Database initialization complete!")
    print(f"Database location: {DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
