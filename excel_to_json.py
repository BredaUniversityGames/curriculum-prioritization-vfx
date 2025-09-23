#!/usr/bin/env python3

import pandas as pd
import json
import re

class ExcelSurveyTransformer:
    def __init__(self, excel_file, schema_file):
        self.excel_file = excel_file
        self.schema_file = schema_file
        self.raw_data = None
        self.schema = None
        self.transformed_data = []
        
        # Load Excel data and schema
        self._load_data()
        self._load_schema()
        
    def _load_data(self):
        """Load Excel data using pandas"""
        try:
            self.raw_data = pd.read_excel(self.excel_file, sheet_name=0)
            print(f"Loaded {len(self.raw_data)} rows from Excel file")
            print(f"Columns: {list(self.raw_data.columns)}")
        except Exception as e:
            print(f"Error loading Excel file: {e}")
            raise
            
    def _load_schema(self):
        """Load schema from JSON file"""
        try:
            with open(self.schema_file, 'r', encoding='utf-8') as f:
                self.schema = json.load(f)
            print(f"Loaded schema with {len(self.schema['questions'])} questions")
        except Exception as e:
            print(f"Error loading schema file: {e}")
            raise
    
    def _clean_value(self, value):
        """Clean and normalize values"""
        if pd.isna(value):
            return None
        
        # Convert to string and clean
        cleaned = str(value).strip()
        
        # Remove common artifacts
        cleaned = cleaned.replace('"', '')
        cleaned = cleaned.replace('\\', '')
        
        # Return None for empty strings
        if not cleaned or cleaned.lower() in ['', 'nan', 'none', '-']:
            return None
            
        return cleaned
    
    def _normalize_question_text(self, text):
        """Normalize question text for matching"""
        if not text:
            return ""
        # Remove extra whitespace, normalize case
        normalized = re.sub(r'\s+', ' ', text.strip().lower())
        # Remove punctuation for fuzzy matching
        normalized = re.sub(r'[^\w\s]', '', normalized)
        return normalized
    
    def _find_column_for_question(self, schema_question):
        """Find Excel column that matches the schema question"""
        schema_normalized = self._normalize_question_text(schema_question)
        
        # Try exact match first
        for col in self.raw_data.columns:
            if col.strip() == schema_question:
                return col
        
        # Try normalized fuzzy matching
        for col in self.raw_data.columns:
            col_normalized = self._normalize_question_text(col)
            if col_normalized == schema_normalized:
                return col
            
        # Try partial matching (schema question contained in column)
        for col in self.raw_data.columns:
            col_normalized = self._normalize_question_text(col)
            if schema_normalized in col_normalized or col_normalized in schema_normalized:
                return col
                
        return None
    
    def _process_multiple_choice(self, value, question_info):
        """Process multiple choice responses (semicolon-separated)"""
        if not value:
            return None
            
        # Split by semicolon and clean each part
        parts = [self._clean_value(part) for part in str(value).split(';')]
        # Remove empty parts
        clean_parts = [part for part in parts if part]
        
        return clean_parts if clean_parts else None
    
    def _skip_metadata_columns(self, col_name):
        """Check if column should be skipped (metadata columns)"""
        metadata_indicators = [
            'id', 'start time', 'completion time', 'email', 'name',
            'timestamp', 'response id', 'ip address'
        ]
        return any(indicator in col_name.lower() for indicator in metadata_indicators)
    
    def transform(self):
        """Transform Excel data according to schema"""
        print("Starting transformation...")
        
        self.transformed_data = []
        
        # Get the questions from schema
        questions = self.schema.get('questions', {})
        
        # Process each row of data
        for index, row in self.raw_data.iterrows():
            transformed_row = {
                'response_id': index + 1  # Add a response ID
            }
            
            # Process each question in the schema
            for question_key, question_info in questions.items():
                question_text = question_info.get('question', '')
                question_type = question_info.get('type', '')
                
                # Find the corresponding column in Excel
                column_name = self._find_column_for_question(question_text)
                
                if column_name is None:
                    print(f"Warning: No column found for question '{question_text}'")
                    transformed_row[question_key] = None
                    continue
                
                if self._skip_metadata_columns(column_name):
                    continue
                    
                # Get the raw value
                raw_value = row.get(column_name)
                
                # Process based on question type
                if question_type == 'multiple_choice':
                    transformed_row[question_key] = self._process_multiple_choice(raw_value, question_info)
                elif question_type == 'open_text':
                    cleaned_value = self._clean_value(raw_value)
                    transformed_row[question_key] = cleaned_value
                elif question_type == 'single_choice':
                    cleaned_value = self._clean_value(raw_value)
                    transformed_row[question_key] = cleaned_value
                elif question_type == 'identifier':
                    # Special handling for ID fields
                    cleaned_value = self._clean_value(raw_value)
                    transformed_row[question_key] = cleaned_value
                else:
                    # Default processing
                    cleaned_value = self._clean_value(raw_value)
                    transformed_row[question_key] = cleaned_value
            
            self.transformed_data.append(transformed_row)
        
        print(f"Transformed {len(self.transformed_data)} responses")
    
    def save_json(self, output_file):
        """Save transformed data to JSON file"""
        try:
            output_data = {
                'survey_metadata': self.schema.get('survey_metadata', {}),
                'responses': self.transformed_data
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            print(f"Saved transformed data to {output_file}")
        except Exception as e:
            print(f"Error saving JSON file: {e}")
            raise
    
    def get_summary(self):
        """Get transformation summary"""
        if not self.transformed_data:
            return "No data transformed yet"
        
        summary = {
            'total_responses': len(self.transformed_data),
            'total_questions': len(self.schema.get('questions', {})),
            'sample_response': self.transformed_data[0] if self.transformed_data else None
        }
        return summary

def main():
    print("Excel Survey Transformer v2.0\n")
    
    # Configuration
    excel_file = "responses.xlsx"
    schema_file = "questions.json"
    output_file = "responses.json"
    
    try:
        # Initialize transformer
        transformer = ExcelSurveyTransformer(excel_file, schema_file)
        
        # Transform the data
        transformer.transform()
        
        # Save to JSON
        transformer.save_json(output_file)
        
        # Print summary
        summary = transformer.get_summary()
        print(f"\nTransformation Summary:")
        print(f"- Total responses: {summary['total_responses']}")
        print(f"- Total questions: {summary['total_questions']}")
        
        print(f"\nTransformation complete! Check {output_file} for results.")
        
    except Exception as e:
        print(f"Error during transformation: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())