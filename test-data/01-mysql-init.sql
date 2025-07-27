-- Sample MySQL test data
-- This will be executed when the MySQL container starts

CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    profile_data JSON,
    INDEX idx_username (username),
    INDEX idx_email (email)
);

CREATE TABLE IF NOT EXISTS categories (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    parent_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    category_id INT,
    created_by INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    metadata JSON,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE SET NULL,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
    INDEX idx_category (category_id),
    INDEX idx_price (price)
);

CREATE TABLE IF NOT EXISTS orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shipping_address TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS order_items (
    id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE RESTRICT,
    UNIQUE KEY unique_order_product (order_id, product_id)
);

-- Insert sample data
INSERT INTO users (username, email, password_hash, profile_data) VALUES
('admin', 'admin@example.com', '$2b$12$hash1', '{"role": "admin", "preferences": {"theme": "dark"}}'),
('john_doe', 'john@example.com', '$2b$12$hash2', '{"role": "user", "preferences": {"theme": "light"}}'),
('jane_smith', 'jane@example.com', '$2b$12$hash3', '{"role": "user", "preferences": {"notifications": true}}');

INSERT INTO categories (name, description, parent_id) VALUES
('Electronics', 'Electronic devices and accessories', NULL),
('Computers', 'Desktop and laptop computers', 1),
('Smartphones', 'Mobile phones and accessories', 1),
('Books', 'Physical and digital books', NULL),
('Fiction', 'Fiction books', 4),
('Non-Fiction', 'Non-fiction books', 4);

INSERT INTO products (name, description, price, category_id, created_by, metadata) VALUES
('Laptop Pro', 'High-performance laptop', 1299.99, 2, 1, '{"brand": "TechCorp", "warranty": "2 years"}'),
('Smartphone X', 'Latest smartphone model', 899.99, 3, 1, '{"brand": "PhoneCorp", "storage": "128GB"}'),
('Programming Guide', 'Complete programming guide', 49.99, 6, 2, '{"pages": 500, "format": "paperback"}'),
('Sci-Fi Novel', 'Best-selling science fiction', 14.99, 5, 2, '{"author": "Famous Writer", "genre": "sci-fi"}');

INSERT INTO orders (user_id, total_amount, status, shipping_address) VALUES
(2, 1349.98, 'delivered', '123 Main St, City, State 12345'),
(3, 64.98, 'shipped', '456 Oak Ave, Town, State 67890'),
(2, 899.99, 'processing', '123 Main St, City, State 12345');

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 1, 1299.99),
(1, 4, 1, 49.99),
(2, 3, 1, 49.99),
(2, 4, 1, 14.99),
(3, 2, 1, 899.99);
