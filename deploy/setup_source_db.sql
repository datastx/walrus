CREATE TABLE public.users (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT,
    created_at  TIMESTAMPTZ DEFAULT now(),
    metadata    JSONB
);

CREATE TABLE public.orders (
    id          SERIAL PRIMARY KEY,
    user_id     INT REFERENCES public.users(id),
    amount      NUMERIC(10, 2) NOT NULL,
    status      TEXT DEFAULT 'pending',
    ordered_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE public.products (
    product_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    price       NUMERIC(10, 2),
    tags        TEXT[],
    description TEXT
);

INSERT INTO public.users (name, email, metadata)
SELECT
    'user_' || i,
    'user_' || i || '@example.com',
    jsonb_build_object('tier', CASE WHEN i % 3 = 0 THEN 'gold' ELSE 'silver' END)
FROM generate_series(1, 10000) AS i;

INSERT INTO public.orders (user_id, amount, status)
SELECT
    (i % 10000) + 1,
    round((random() * 1000)::numeric, 2),
    CASE (i % 4) WHEN 0 THEN 'pending' WHEN 1 THEN 'shipped'
                 WHEN 2 THEN 'delivered' ELSE 'cancelled' END
FROM generate_series(1, 50000) AS i;

INSERT INTO public.products (name, price, tags, description)
SELECT
    'product_' || i,
    round((random() * 500)::numeric, 2),
    ARRAY['tag_' || (i % 5), 'tag_' || (i % 10)],
    repeat('description text ', 100)
FROM generate_series(1, 5000) AS i;

ALTER USER replicator WITH REPLICATION;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
GRANT USAGE ON SCHEMA public TO replicator;
