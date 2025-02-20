import pandas as pd
import os
import glob
from multiprocessing import Pool
import urllib.parse

input_folder = 'Datasets/pre_ans_sorted_graph'
graph_folder = os.path.join(input_folder, 'graphs')
output_folder = 'Datasets/ans_all'
os.makedirs(output_folder, exist_ok=True)
html_folder = os.path.join(output_folder, 'html_files')
os.makedirs(html_folder, exist_ok=True)

key_columns = ['name', 'item_type', 'item_form', 'prescription', 'manufacturer', 'country']

def process_file(file_path):
    try:
        competitor_name = os.path.splitext(os.path.basename(file_path))[0].split('diff_')[-1]
        df = pd.read_excel(file_path)

        required_columns = key_columns + ['Медианная цена', 'Индекс изменений', 'Сегменты',
                                          'Частота изменений в минус', 'Заработали', 'Название HTML']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            print(f"Отсутствуют колонки в {file_path}: {missing}")
            return pd.DataFrame()

        df['Ссылка на график'] = df['Название HTML'].apply(
            lambda x: os.path.abspath(os.path.join(graph_folder, str(x).strip())) if pd.notna(x) else ''
        )

        grouped = df.groupby(key_columns).agg({
            'Медианная цена': 'median',
            'Индекс изменений': 'sum',
            'Частота изменений в минус': 'sum',
            'Заработали': 'sum',
            'Сегменты': lambda x: ';'.join(set(x.dropna().astype(str))),
            'Ссылка на график': 'first'
        }).reset_index()

        grouped['Данные конкурента'] = grouped.apply(
            lambda row: (
                f"Название конкурента: {competitor_name}; "
                f"Медианная цена: {row['Медианная цена']:.2f}; "
                f"Индекс изменений: {row['Индекс изменений']}; "
                f"Сегменты: {row['Сегменты']}; "
                f"Частота изменений в минус: {row['Частота изменений в минус']}; "
                f"Заработали: {row['Заработали']}"
            ),
            axis=1
        )

        grouped['Название конкурента'] = competitor_name
        grouped['Как часто в аптеках'] = 1
        return grouped

    except Exception as e:
        print(f"Ошибка в {file_path}: {str(e)}")
        return pd.DataFrame()

excel_files = glob.glob(os.path.join(input_folder, '*.xlsx'))
with Pool() as pool:
    data_frames = [df for df in pool.map(process_file, excel_files) if not df.empty]

if not data_frames:
    print("Нет данных для обработки")
    exit()

combined_df = pd.concat(data_frames, ignore_index=True)
final_df = combined_df.groupby(key_columns).agg({
    'Медианная цена': 'mean',
    'Индекс изменений': 'sum',
    'Частота изменений в минус': 'sum',
    'Заработали': 'sum',
    'Как часто в аптеках': 'sum',
    'Данные конкурента': list,
    'Название конкурента': lambda x: list(set(x)),
    'Ссылка на график': list
}).reset_index()

def create_html_file(row, index):
    try:
        html_content = '''<html><head><meta charset="UTF-8"><style>
            body{font-family:Arial}.competitor{margin-bottom:20px;padding:15px;border:1px solid #ddd}
            .open-button{padding:10px 20px;background:#3498DB;color:#fff;border-radius:4px;text-decoration:none}
            .open-button:hover{background:#2980B9}</style></head><body><h2>Анализ конкурентов</h2>'''

        for data, graph_path in zip(row['Данные конкурента'], row['Ссылка на график']):
            html_content += '<div class="competitor">'
            lines = data.replace('; ', '\n').split('\n')

            for line in lines:
                if ': ' in line:
                    key, value = line.split(': ', 1)
                    if key == "Название конкурента":
                        html_content += f'<div style="font-weight:bold;font-size:20px">{value}</div>'
                    else:
                        html_content += f'<p><b>{key}:</b> {value}</p>'

            if pd.notna(graph_path) and os.path.exists(graph_path):
                abs_path = os.path.abspath(graph_path)
                encoded_path = urllib.parse.quote(abs_path)
                file_uri = f'file:///{encoded_path}'
                html_content += f'<a href="{file_uri}" class="open-button" target="_blank">📈 Открыть график</a>'
            else:
                html_content += f'<p style="color:red">График недоступен (Path: {graph_path})</p>'

            html_content += '</div>'

        html_content += '</body></html>'
        html_path = os.path.join(html_folder, f'competitor_data_{index}.html')
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        return os.path.relpath(html_path, start=output_folder)

    except Exception as e:
        print(f"Ошибка HTML для записи {index}: {str(e)}")
        return ''

final_df['Ссылка на данные'] = final_df.apply(lambda row: create_html_file(row, row.name), axis=1)
final_df['Ссылка на данные'] = final_df['Ссылка на данные'].apply(
    lambda p: f'=HYPERLINK("{p.replace("\\\\", "/")}", "📊 Данные")' if p else '')

output_file = os.path.join(output_folder, 'ans_all.xlsx')
final_df.to_excel(output_file, index=False, engine='openpyxl')
print(f"Файл сохранён: {output_file}\nСоздано HTML-страниц: {len(final_df)}")
