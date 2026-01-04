-- SQL Exercises Database Schema
-- This file contains DDL for all tables across all three datasets:
-- 1. Employees (HR data)
-- 2. Ecommerce (transactional data)
-- 3. Analytics (event/session data)

-- Note: Foreign key relationships are documented in comments but not enforced
-- to simplify table creation order. Data integrity is maintained by the
-- data generation scripts.

-- ============================================================
-- EMPLOYEES DATABASE (HR Data)
-- ============================================================

CREATE TABLE IF NOT EXISTS departments (
    department_id INTEGER PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL,
    location VARCHAR(100),
    budget DECIMAL(15, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS employees (
    employee_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    hire_date DATE NOT NULL,
    job_title VARCHAR(100),
    salary DECIMAL(10, 2),
    commission_pct DECIMAL(4, 2),
    manager_id INTEGER,          -- References employees(employee_id)
    department_id INTEGER,       -- References departments(department_id)
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS salary_history (
    history_id INTEGER PRIMARY KEY,
    employee_id INTEGER,         -- References employees(employee_id)
    old_salary DECIMAL(10, 2),
    new_salary DECIMAL(10, 2),
    change_date DATE NOT NULL,
    change_reason VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS projects (
    project_id INTEGER PRIMARY KEY,
    project_name VARCHAR(200) NOT NULL,
    start_date DATE,
    end_date DATE,
    budget DECIMAL(15, 2),
    status VARCHAR(20) CHECK (status IN ('planning', 'active', 'completed', 'cancelled'))
);

CREATE TABLE IF NOT EXISTS project_assignments (
    assignment_id INTEGER PRIMARY KEY,
    employee_id INTEGER,         -- References employees(employee_id)
    project_id INTEGER,          -- References projects(project_id)
    role VARCHAR(50),
    hours_allocated INTEGER,
    start_date DATE,
    end_date DATE
);

CREATE TABLE IF NOT EXISTS performance_reviews (
    review_id INTEGER PRIMARY KEY,
    employee_id INTEGER,         -- References employees(employee_id)
    reviewer_id INTEGER,         -- References employees(employee_id)
    review_date DATE NOT NULL,
    rating INTEGER CHECK (rating BETWEEN 1 AND 5),
    comments TEXT
);

-- ============================================================
-- ECOMMERCE DATABASE (Transactional Data)
-- ============================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    customer_tier VARCHAR(20) CHECK (customer_tier IN ('bronze', 'silver', 'gold', 'platinum'))
);

CREATE TABLE IF NOT EXISTS addresses (
    address_id INTEGER PRIMARY KEY,
    customer_id INTEGER,         -- References customers(customer_id)
    address_type VARCHAR(20) CHECK (address_type IN ('billing', 'shipping')),
    street_address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100) DEFAULT 'USA',
    is_default BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS categories (
    category_id INTEGER PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    parent_category_id INTEGER,  -- References categories(category_id)
    description TEXT
);

CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    category_id INTEGER,         -- References categories(category_id)
    unit_price DECIMAL(10, 2) NOT NULL,
    cost_price DECIMAL(10, 2),
    stock_quantity INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,         -- References customers(customer_id)
    order_date TIMESTAMP NOT NULL,
    shipping_address_id INTEGER, -- References addresses(address_id)
    billing_address_id INTEGER,  -- References addresses(address_id)
    order_status VARCHAR(20) CHECK (order_status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned')),
    subtotal DECIMAL(12, 2),
    tax_amount DECIMAL(10, 2),
    shipping_cost DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2) DEFAULT 0,
    total_amount DECIMAL(12, 2),
    payment_method VARCHAR(50),
    notes TEXT
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id INTEGER PRIMARY KEY,
    order_id INTEGER,            -- References orders(order_id)
    product_id INTEGER,          -- References products(product_id)
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount_pct DECIMAL(5, 2) DEFAULT 0,
    line_total DECIMAL(12, 2)
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id INTEGER PRIMARY KEY,
    product_id INTEGER,          -- References products(product_id)
    customer_id INTEGER,         -- References customers(customer_id)
    rating INTEGER CHECK (rating BETWEEN 1 AND 5),
    review_title VARCHAR(200),
    review_text TEXT,
    review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_verified_purchase BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS promotions (
    promotion_id INTEGER PRIMARY KEY,
    promo_code VARCHAR(50) UNIQUE,
    description VARCHAR(255),
    discount_type VARCHAR(20) CHECK (discount_type IN ('percentage', 'fixed_amount', 'free_shipping')),
    discount_value DECIMAL(10, 2),
    min_order_amount DECIMAL(10, 2),
    start_date DATE,
    end_date DATE,
    usage_limit INTEGER,
    times_used INTEGER DEFAULT 0
);

-- ============================================================
-- ANALYTICS DATABASE (Event/Session Data)
-- ============================================================

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    anonymous_id VARCHAR(100),
    email VARCHAR(255),
    signup_date DATE,
    signup_source VARCHAR(50),
    country VARCHAR(100),
    device_type VARCHAR(20),
    is_premium BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id VARCHAR(100) PRIMARY KEY,
    user_id INTEGER,             -- References users(user_id)
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

CREATE TABLE IF NOT EXISTS page_views (
    view_id INTEGER PRIMARY KEY,
    session_id VARCHAR(100),     -- References sessions(session_id)
    user_id INTEGER,             -- References users(user_id)
    page_url VARCHAR(500) NOT NULL,
    page_title VARCHAR(255),
    view_timestamp TIMESTAMP NOT NULL,
    time_on_page_seconds INTEGER,
    scroll_depth_pct INTEGER
);

CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY,
    session_id VARCHAR(100),     -- References sessions(session_id)
    user_id INTEGER,             -- References users(user_id)
    event_name VARCHAR(100) NOT NULL,
    event_category VARCHAR(100),
    event_timestamp TIMESTAMP NOT NULL,
    event_properties VARCHAR(1000),  -- JSON stored as string for simplicity
    page_url VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS conversions (
    conversion_id INTEGER PRIMARY KEY,
    user_id INTEGER,             -- References users(user_id)
    session_id VARCHAR(100),     -- References sessions(session_id)
    conversion_type VARCHAR(50) NOT NULL,
    conversion_value DECIMAL(12, 2),
    conversion_timestamp TIMESTAMP NOT NULL,
    attribution_channel VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS ab_tests (
    test_id INTEGER PRIMARY KEY,
    test_name VARCHAR(200) NOT NULL,
    start_date DATE,
    end_date DATE,
    status VARCHAR(20) CHECK (status IN ('draft', 'running', 'completed', 'stopped'))
);

CREATE TABLE IF NOT EXISTS ab_test_assignments (
    assignment_id INTEGER PRIMARY KEY,
    test_id INTEGER,             -- References ab_tests(test_id)
    user_id INTEGER,             -- References users(user_id)
    variant VARCHAR(50) NOT NULL,
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date DATE NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15, 4),
    segment VARCHAR(100),
    PRIMARY KEY (metric_date, metric_name, segment)
);
