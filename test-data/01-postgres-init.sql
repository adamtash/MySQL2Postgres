-- PostgreSQL initialization script
-- This will be executed when the PostgreSQL container starts

-- Enable extensions that might be useful
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create equivalent PostgreSQL schema for migration testing
-- Note: Tables use PascalCase naming to match typical PostgreSQL conventions

CREATE TABLE IF NOT EXISTS "Users" (
    "id" SERIAL PRIMARY KEY,
    "username" VARCHAR(50) NOT NULL UNIQUE,
    "email" VARCHAR(100) NOT NULL,
    "password_hash" VARCHAR(255) NOT NULL,
    "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "is_active" BOOLEAN DEFAULT TRUE,
    "profile_data" JSONB
);

CREATE INDEX IF NOT EXISTS "idx_users_username" ON "Users" ("username");
CREATE INDEX IF NOT EXISTS "idx_users_email" ON "Users" ("email");

CREATE TABLE IF NOT EXISTS "Categories" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(100) NOT NULL,
    "description" TEXT,
    "parent_id" INTEGER,
    "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY ("parent_id") REFERENCES "Categories"("id") ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS "Products" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(200) NOT NULL,
    "description" TEXT,
    "price" DECIMAL(10,2) NOT NULL,
    "category_id" INTEGER,
    "created_by" INTEGER,
    "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "metadata" JSONB,
    FOREIGN KEY ("category_id") REFERENCES "Categories"("id") ON DELETE SET NULL,
    FOREIGN KEY ("created_by") REFERENCES "Users"("id") ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS "idx_products_category" ON "Products" ("category_id");
CREATE INDEX IF NOT EXISTS "idx_products_price" ON "Products" ("price");

-- Create custom type for order status
CREATE TYPE order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled');

CREATE TABLE IF NOT EXISTS "Orders" (
    "id" SERIAL PRIMARY KEY,
    "user_id" INTEGER NOT NULL,
    "total_amount" DECIMAL(10,2) NOT NULL,
    "status" order_status DEFAULT 'pending',
    "order_date" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "shipping_address" TEXT,
    FOREIGN KEY ("user_id") REFERENCES "Users"("id") ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS "OrderItems" (
    "id" SERIAL PRIMARY KEY,
    "order_id" INTEGER NOT NULL,
    "product_id" INTEGER NOT NULL,
    "quantity" INTEGER NOT NULL DEFAULT 1,
    "unit_price" DECIMAL(10,2) NOT NULL,
    FOREIGN KEY ("order_id") REFERENCES "Orders"("id") ON DELETE CASCADE,
    FOREIGN KEY ("product_id") REFERENCES "Products"("id") ON DELETE RESTRICT,
    UNIQUE ("order_id", "product_id")
);

-- Note: For migration testing, we create the schema but leave tables empty
-- This allows the migration tool to populate the PostgreSQL database from MySQL data
-- Uncomment the INSERT statements below if you need test data in PostgreSQL

/*
-- Insert test data (commented out for migration testing)
INSERT INTO "Users" ("Username", "Email", "PasswordHash", "IsActive", "ProfileData") VALUES
('john_doe', 'john@example.com', 'hashed_password_1', TRUE, '{"theme": "dark", "notifications": true}'),
('jane_smith', 'jane@example.com', 'hashed_password_2', TRUE, '{"theme": "light", "notifications": false}'),
('admin_user', 'admin@example.com', 'hashed_password_3', TRUE, '{"role": "admin", "permissions": ["read", "write", "delete"]}');

INSERT INTO "Categories" ("Name", "Description", "ParentId") VALUES
('Electronics', 'Electronic devices and accessories', NULL),
('Computers', 'Desktop and laptop computers', 1),
('Smartphones', 'Mobile phones and accessories', 1),
('Books', 'Physical and digital books', NULL),
('Fiction', 'Fiction books', 4),
('Non-Fiction', 'Non-fiction books', 4);

INSERT INTO "Products" ("Name", "Description", "Price", "CategoryId", "CreatedBy", "Metadata") VALUES
('Laptop Pro', 'High-performance laptop', 1299.99, 2, 1, '{"brand": "TechCorp", "warranty": "2 years"}'),
('Smartphone X', 'Latest smartphone model', 899.99, 3, 1, '{"brand": "PhoneCorp", "storage": "128GB"}'),
('Programming Guide', 'Complete programming guide', 49.99, 6, 2, '{"pages": 500, "format": "paperback"}'),
('Sci-Fi Novel', 'Best-selling science fiction', 14.99, 5, 2, '{"author": "Famous Writer", "genre": "sci-fi"}');

INSERT INTO "Orders" ("UserId", "TotalAmount", "Status", "ShippingAddress") VALUES
(2, 1349.98, 'delivered', '123 Main St, City, State 12345'),
(3, 64.98, 'shipped', '456 Oak Ave, Town, State 67890'),
(1, 899.99, 'processing', '789 Pine Rd, Village, State 54321');

INSERT INTO "OrderItems" ("OrderId", "ProductId", "Quantity", "UnitPrice") VALUES
(1, 1, 1, 1299.99),
(1, 2, 1, 899.99),
(2, 3, 1, 49.99),
(2, 4, 1, 14.99),
(3, 2, 1, 899.99);
*/
