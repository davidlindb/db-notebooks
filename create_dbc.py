import os
import random
import string
import zipfile
import argparse
import shutil

def generate_random_file(file_name, size_in_mb, rand, ext, result_size):
    """
    Generate a file with random content of a specified size in MB.
    """
    with open(file_name, 'w') as f:
        if (ext == 'dbc'):
            f.write('{"version":"NotebookV1","origId":3043243669922456,"name":"Untitled Notebook 2024-09-24 11:27:38","language":"sql","commands":[{"version":"CommandV1","origId":3043243669922457,"guid":"68255229-76ab-44f8-8105-2378ce0c56f0","subtype":"command","commandType":"auto","position":1.0,"command":"')
        else:
            f.write('# Databricks notebook source\n')
        for _ in range(int(size_in_mb * 1024 * 1024)):
            if rand:
                f.write(random.choice(string.ascii_letters))
            else:
                f.write('x')
        if (ext == 'dbc'):
            f.write('","commandVersion":0,"state":"input","results":')
            if result_size > 0:
                f.write('"')
                for _ in range(int(result_size * 1024 * 1024)):
                    if rand:
                        f.write(random.choice(string.ascii_letters))
                    else:
                        f.write('x')
                f.write('"')
            else:
                f.write('null')
            f.write(',"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"errorDetails":null,"baseErrorDetails":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[],"subcommandOptions":null,"contentSha256Hex":null,"tableResultSettingsMap":null,"nuid":"27734df0-22a3-418e-9fd7-c0b41b65b8c3"}],"dashboards":[],"guid":"c5efa3c8-af8a-4764-bd07-2e6f78eb44b7","globalVars":{},"iPythonMetadata":null,"inputWidgets":{},"notebookMetadata":{"pythonIndentUnit":4},"reposExportFormat":"SOURCE","environmentMetadata":{"client":"1","base_environment":""}}')

def create_zip_file(zip_name, file_name):
    """
    Compress a file into a zip archive.
    """
    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(file_name)

def main(ext, size, rand, result_size):
    file_name = 'notebook1.sql'
    randStr = "rand" if rand else "notrand"
    zip_name = f'{size}mb_{randStr}.{ext}'
    
    # Step 1: Generate a file with random content of the specified size
    generate_random_file(file_name, size, rand, ext, result_size)
    
    # Step 2: Compress the file into a zip file
    if ext == 'sql':
        shutil.copy(file_name, zip_name)
    else:
        create_zip_file(zip_name, file_name)
        with zipfile.ZipFile(zip_name, 'r') as zipf:
            uncompressed_size = sum([zinfo.file_size for zinfo in zipf.infolist()])
    
        print(f"Desired decompressed size: size MB")
        print(f"Actual decompressed size: {uncompressed_size / (1024 * 1024):.2f} MB")
            
    # Clean up the temporary file
    os.remove(file_name)

if __name__ == '__main__':
    
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Process three optional command-line arguments.")
    
    # Add optional arguments x, y, z
    parser.add_argument('-ext', type=str, help='dbc or zip', default='dbc')
    parser.add_argument('-size', type=str, help='size in MB', default='9')
    parser.add_argument('-rand', type=str, help='True or False - random content, otherwise constant', default='False')
    parser.add_argument('-result_size', type=str, help='size in MB', default='0')
    
    args = parser.parse_args()
    
    main(args.ext, float(args.size), args.rand == 'True', float(args.result_size))

