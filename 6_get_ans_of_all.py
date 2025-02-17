import os
import re
import pandas as pd
import plotly.graph_objects as go

# Input directory containing the Excel files
input_dir = "Datasets/pre_ans_sorted_new"

# Output directory for updated Excel files
output_dir = "Datasets/pre_ans_sorted_graph"
os.makedirs(output_dir, exist_ok=True)

# Directory for saving HTML graphs
graphs_dir = os.path.join(output_dir, "graphs")
os.makedirs(graphs_dir, exist_ok=True)

# Get a list of all Excel files in the input directory
excel_files = [file for file in os.listdir(input_dir) if file.endswith((".xlsx", ".xls"))]
total_files = len(excel_files)

print(f"Found {total_files} Excel files to process.")

# Iterate through each Excel file
for i, file_name in enumerate(excel_files, start=1):
    file_path = os.path.join(input_dir, file_name)
    print(f"Processing file {i}/{total_files}: {file_name}...")

    # Read the Excel file into a pandas DataFrame
    df = pd.read_excel(file_path)

    # Extract columns related to "Количество"
    quantity_columns = [col for col in df.columns if "Количество" in col]

    # Initialize the new column for graph links
    df["Ссылка на график"] = ""

    # Iterate through each row to calculate dynamics and generate a graph if needed
    for index, row in df.iterrows():
        quantity_values = row[quantity_columns].values

        # Check for meaningful dynamics (e.g., non-constant values)
        if len(set(quantity_values)) > 1:
            # Extract date and time from the column names and format as MM-DD_HH-MM
            formatted_dates = []
            for col in quantity_columns:
                # Use regex to extract the date and time
                match = re.search(r'Количество.*_(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})(?:.*?)$', col)
                if match:
                    # Extract month, day, hour, minute
                    month = match.group(2)
                    day = match.group(3)
                    hour = match.group(4)
                    minute = match.group(5)
                    formatted_date = f"{month}-{day}_{hour}-{minute}"
                    formatted_dates.append(formatted_date)
                else:
                    # Handle cases where the pattern doesn't match
                    formatted_dates.append('')

            # Create a DataFrame for plotting
            plot_df = pd.DataFrame({
                'Дата и время': formatted_dates,
                'Количество': quantity_values
            })

            # Exclude any rows with empty date strings
            plot_df = plot_df[plot_df['Дата и время'] != '']

            # Convert to datetime for sorting
            plot_df['Дата и время_DT'] = pd.to_datetime(plot_df['Дата и время'], format="%m-%d_%H-%M")

            # Sort the DataFrame by date
            plot_df.sort_values('Дата и время_DT', inplace=True)

            # Use smooth lines for the plot
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=plot_df['Дата и время'],
                y=plot_df['Количество'],
                mode='lines+markers',
                name='Количество',
                line_shape='spline',
                line=dict(smoothing=1.3),
                marker=dict(size=6)
            ))

            # Update layout for visual appeal
            fig.update_layout(
                title=f"Динамика количества - Строка {index}",
                xaxis_title="Дата и время",
                yaxis_title="Количество",
                xaxis=dict(tickangle=45),
                template='plotly_white',
                width=800,
                height=500
            )

            # Save the figure as an HTML file
            graph_file_name = f"{file_name.replace('.xlsx', '').replace('.xls', '')}_row_{index}_graph.html"
            graph_file_path = os.path.join(graphs_dir, graph_file_name)
            fig.write_html(graph_file_path)

            # Add the link to the graph in the DataFrame
            # For LibreOffice, we can use relative paths
            relative_graph_path = os.path.join("graphs", graph_file_name)
            # Create a hyperlink formula (LibreOffice should recognize the HYPERLINK formula)
            link = '=HYPERLINK("{}", "{}")'.format(relative_graph_path, "Открыть график")
            df.at[index, "Ссылка на график"] = link

    # Save the updated DataFrame to the new output directory
    output_file_path = os.path.join(output_dir, f"updated_{file_name}")
    # Ensure that hyperlinks are written correctly by specifying `engine` and `options`
    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Data', na_rep='')
        workbook = writer.book
        worksheet = writer.sheets['Data']

        # Format the "Ссылка на график" column as hyperlinks
        link_format = workbook.add_format({'font_color': 'blue', 'underline': 1})
        for row_num, cell_value in enumerate(df['Ссылка на график'], start=1):  # start=1 to account for header row
            worksheet.write_formula(row_num, df.columns.get_loc('Ссылка на график'), cell_value, link_format)

    print(f"File {i}/{total_files} processed and saved as: {output_file_path}")

print(f"All files have been processed and saved in: {output_dir}")
