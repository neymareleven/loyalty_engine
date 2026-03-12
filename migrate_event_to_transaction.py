import os
import glob
import re

# Base path
base_dir = r"c:\Users\gaetan.feutse\Documents\Tools\CascadeProjects\windsurf-project\admin_dashboard\src\scenes\fidelite"

# Replacements to apply
replacements = [
    ("event_type", "transaction_type"),
    ("Event_type", "Transaction_type"),
    ("event-types", "transaction-types"),
    ("Event-types", "Transaction-types"),
    ("eventType", "transactionType"),
    ("eventTypes", "transactionTypes"),
    ("EventTypes", "TransactionTypes"),
    ("EventType", "TransactionType"),
    ("Event Types", "Transaction Types"),
    ("Event Type", "Transaction Type"),
    ("Type d'événement", "Type de transaction"),
    ("type d'événement", "type de transaction"),
    ("Type d'événement (INTERNAL)", "Type de transaction (INTERNAL)"),
]

# File renaming
def rename_files(start_dir):
    for root, dirs, files in os.walk(start_dir, topdown=False):
        # Rename files
        for name in files:
            if name.endswith('.jsx') or name.endswith('.js'):
                new_name = name.replace("EventType", "TransactionType").replace("eventType", "transactionType")
                if new_name != name:
                    os.rename(os.path.join(root, name), os.path.join(root, new_name))
        
        # Rename directories
        for name in dirs:
            new_name = name.replace("eventTypes", "transactionTypes").replace("eventType", "transactionType")
            if new_name != name:
                os.rename(os.path.join(root, name), os.path.join(root, new_name))

# Content replacing
def replace_content(start_dir):
    for root, dirs, files in os.walk(start_dir):
        for name in files:
            if name.endswith('.jsx') or name.endswith('.js'):
                file_path = os.path.join(root, name)
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                new_content = content
                
                # Special cases for imports
                new_content = new_content.replace("components/eventTypes", "components/transactionTypes")

                for old, new in replacements:
                    new_content = new_content.replace(old, new)

                if new_content != content:
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(new_content)
                    print(f"Updated {file_path}")

print("Renaming directories and files...")
rename_files(base_dir)

print("\nUpdating file contents...")
replace_content(base_dir)

print("\nMigration completed successfully.")
