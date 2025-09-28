import pandas as pd
import os

# CSV oku
csv_path = r"D:\lakehouse_demo\duckdb\data\customers-2000000.csv"
df = pd.read_csv(csv_path)

# Hedef boyut (2 GB)
target_size = 2 * 1024**3  # bayt cinsinden

# Mevcut CSV boyutu (MB/GB)
current_size = os.path.getsize(csv_path)
print(f"Orijinal CSV boyutu: {current_size/1024**2:.2f} MB")

# Kaç kere tekrarlamak gerekiyor?
repeat_factor = int(target_size // current_size) + 1
print(f"Tekrarlama katsayısı: {repeat_factor}")

# Veriyi çoğalt
big_df = pd.concat([df] * 30, ignore_index=True)

# Parquet kaydet
big_df.to_parquet("D:\lakehouse_demo\duckdb\data\customers-2gb.parquet", engine="pyarrow", index=False)
print("customers-2gb.parquet oluşturuldu ✅")
