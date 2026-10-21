import os
import random
import time
import uuid
import pandas as pd


def process_csv_to_parquet(
    csv_input_path="2019-Nov.csv",
    output_dir="data",
    target_10m=10_000_000,
):
  """Lee el CSV original pesado (o genera sintético si no existe), inyecta los atributos

  del pipeline (prioridades, IPs, flags de fraude) y exporta los 5 cortes Parquet (1K, 10K, 100K, 1M, 10M).
  """
  os.makedirs(output_dir, exist_ok=True)
  parquet_10m_path = os.path.join(output_dir, "ecommerce_10M.parquet")

  start_time = time.time()
  priorities = {
      "payment_alert": 1,
      "refund": 2,
      "purchase": 3,
      "cart": 4,
      "view": 5,
  }

  if os.path.exists(csv_input_path):
    print(
        f"\n⏳ Leyendo y procesando '{csv_input_path}' hacia Parquet"
        f" ({target_10m:,} filas)..."
    )
    chunk_size = 1_000_000
    total_processed = 0
    processed_chunks = []

    for chunk in pd.read_csv(csv_input_path, chunksize=chunk_size):
      needed = min(len(chunk), target_10m - total_processed)
      df_block = chunk.iloc[:needed].copy()

      df_block["event_id"] = [
          str(uuid.uuid4())[:8] for _ in range(len(df_block))
      ]
      is_fraud_list = [random.random() < 0.015 for _ in range(len(df_block))]
      df_block["is_fraud"] = is_fraud_list

      ip_list = []
      event_types = []
      priority_list = []

      for _, row in df_block.iterrows():
        if row["is_fraud"]:
          evt = "payment_alert"
          ip = random.choice(["10.0.0.66", "10.0.0.99"])
        else:
          evt = row["event_type"] if pd.notna(row["event_type"]) else "view"
          if evt == "payment_alert":
            evt = "view"
          ip = f"192.168.{random.randint(1, 50)}.{random.randint(1, 254)}"

        prio = priorities.get(evt, 5)
        event_types.append(evt)
        ip_list.append(ip)
        priority_list.append(prio)

      df_block["event_type"] = event_types
      df_block["ip_address"] = ip_list
      df_block["priority"] = priority_list

      processed_chunks.append(df_block)
      total_processed += len(df_block)
      print(f"  -> {total_processed:,} / {target_10m:,} filas procesadas...")

      if total_processed >= target_10m:
        break

    df_10m = pd.concat(processed_chunks, ignore_index=True)
  else:
    print(
        f"\n⚠️ No se encontró '{csv_input_path}'. Generando sintéticamente"
        f" {target_10m:,} filas..."
    )
    categories = [
        "electronics.smartphone",
        "electronics.audio",
        "computers.laptop",
        "apparel.shoes",
        "home.appliances",
        "sports.fitness",
    ]
    products = [
        {
            "product_id": p_id,
            "category_code": random.choice(categories),
            "brand": f"Brand_{random.randint(1, 100)}",
            "price": round(random.uniform(15.0, 2000.0), 2),
        }
        for p_id in range(1000, 21000)
    ]
    df_products = pd.DataFrame(products)

    batch_size = 1_000_000
    chunks = []
    for b in range(target_10m // batch_size):
      records = []
      for _ in range(batch_size):
        prod = df_products.sample(1).iloc[0]
        is_fraud = random.random() < 0.015
        if is_fraud:
          evt = "payment_alert"
          ip = random.choice(["10.0.0.66", "10.0.0.99"])
        else:
          evt = random.choices(
              ["view", "cart", "purchase", "refund"], weights=[65, 20, 12, 3]
          )[0]
          ip = f"192.168.{random.randint(1, 50)}.{random.randint(1, 254)}"

        records.append({
            "event_id": str(uuid.uuid4())[:8],
            "event_time": pd.Timestamp.now()
            - pd.Timedelta(seconds=random.randint(0, 3000000)),
            "event_type": evt,
            "product_id": int(prod["product_id"]),
            "category_code": prod["category_code"],
            "brand": prod["brand"],
            "price": float(prod["price"]),
            "user_id": f"user_{random.randint(1000, 50000)}",
            "user_session": str(uuid.uuid4())[:12],
            "ip_address": ip,
            "priority": priorities[evt],
            "is_fraud": is_fraud,
        })
      df_batch = pd.DataFrame(records)
      chunks.append(df_batch)
      print(f"  -> {(b + 1) * batch_size:,} / {target_10m:,} filas generadas...")

    df_10m = pd.concat(chunks, ignore_index=True)

  # Guardar archivo maestro de 10M
  df_10m.to_parquet(parquet_10m_path, compression="snappy", index=False)
  elapsed = round(time.time() - start_time, 2)
  size_mb = round(os.path.getsize(parquet_10m_path) / (1024 * 1024), 2)
  print(
      f"\n✅ Archivo maestro 'ecommerce_10M.parquet' creado ({size_mb} MB) en"
      f" {elapsed}s"
  )

  # Exportar cortes (1K, 10K, 100K, 1M) asegurando escritura completa
  cuts = {"1K": 1_000, "10K": 10_000, "100K": 100_000, "1M": 1_000_000}
  print("\n✂️ Exportando cortes recortados en Parquet (1K, 10K, 100K, 1M)...")

  for name, nrows in cuts.items():
    cut_path = os.path.join(output_dir, f"ecommerce_{name}.parquet")
    df_cut = df_10m.iloc[:nrows].copy()
    df_cut.to_parquet(cut_path, compression="snappy", index=False)
    cut_mb = round(os.path.getsize(cut_path) / (1024 * 1024), 2)
    print(f"  -> Archivo '{cut_path}' guardado ({nrows:,} filas, {cut_mb} MB)")

  print("\n🎉 ¡Conversión completada con éxito!")


if __name__ == "__main__":
  process_csv_to_parquet()