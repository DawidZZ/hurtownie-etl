import pandas as pd
import numpy as np

# Wczytywanie pliku CSV
input_file = 'data/details.csv'  # Zmień tę nazwę na nazwę swojego pliku CSV
df = pd.read_csv(input_file)

# Pobieranie pierwszych 12 rekordów
first_12_records = df.head(12)

# Tworzenie 3 niepoprawnych rekordów z odpowiednią liczbą kolumn
invalid_records = []

for _ in range(3):
    invalid_row = []
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            # Dla kolumn liczbowych, wstawiamy wartość ujemną lub string
            invalid_value = -999 if np.random.rand() > 0.5 else 'Invalid'
        elif pd.api.types.is_string_dtype(df[col]):
            # Dla kolumn tekstowych, wstawiamy NaN
            invalid_value = np.nan
        else:
            # Dla innych typów kolumn (np. daty), wstawiamy 'Invalid'
            invalid_value = 'Invalid'
        invalid_row.append(invalid_value)
    invalid_records.append(invalid_row)

# Konwersja listy niepoprawnych rekordów na DataFrame
invalid_records_df = pd.DataFrame(invalid_records, columns=df.columns)

# Łączenie poprawnych i niepoprawnych rekordów
combined_df = pd.concat([first_12_records, invalid_records_df], ignore_index=True)

# Zapis do nowego pliku CSV
output_file = 'test_details.csv'
combined_df.to_csv(output_file, index=False)



# Wczytywanie pliku CSV
df = pd.read_csv(input_file)

# Usuwanie pierwszych 3 rekordów
df_without_first_3 = df.iloc[3:]

# Zapis do nowego pliku CSV
output_file = 'details_without_3.csv'
df_without_first_3.to_csv(output_file, index=False)

print(f'Nowy plik został zapisany jako {output_file}')


print(f'Nowy plik został zapisany jako {output_file}')
