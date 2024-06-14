import pandas as pd

def analyze_csv(file_path, output_file):
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    # Initialize the results dictionary
    analysis = {
        "Column Name": [],
        "Suggested Type": [],
        "Null Ratio (%)": [],
        "Distinct Value Count": [],
        "Most Common Values (Top 10)": []
    }

    for column in df.columns:
        analysis["Column Name"].append(column)
        
        # Determine suggested type
        suggested_type = pd.api.types.infer_dtype(df[column], skipna=True)
        analysis["Suggested Type"].append(suggested_type)
        
        # Calculate null ratio as a percentage
        null_ratio = df[column].isnull().mean() * 100
        analysis["Null Ratio (%)"].append(null_ratio)
        
        # Count distinct values
        distinct_value_count = df[column].nunique()
        analysis["Distinct Value Count"].append(distinct_value_count)
        
        # Get top 10 most common values
        most_common_values = df[column].value_counts().head(10).to_dict()
        analysis["Most Common Values (Top 10)"].append(most_common_values)
    
    # Convert the results dictionary to a DataFrame
    analysis_df = pd.DataFrame(analysis)
    
    # Save the DataFrame to a CSV file
    analysis_df.to_csv(output_file, index=False)
    print(f"Analysis saved to {output_file}")

# Usage example
input_file_path = 'data/density.csv'  # Replace with your CSV file path
output_file_path = 'file_profiles/density_results.csv'  # Replace with your desired output file path
analyze_csv(input_file_path, output_file_path)
