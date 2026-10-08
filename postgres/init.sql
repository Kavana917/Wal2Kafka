CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL,
    age INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users (name, email, age)
VALUES
    ('Alice', 'alice@example.com', 21),
    ('Bob', 'bob@example.com', 24),
    ('Charlie', 'charlie@example.com', 22);