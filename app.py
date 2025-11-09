import os  
from flask import Flask, request, render_template  
from werkzeug.utils import secure_filename
  
# Import the S3 functions from our other file  
from upload import begin_upload
  
# --- Configuration ---  
# IMPORTANT: This directory is for temporarily storing files before uploading to S3.  
UPLOAD_FOLDER = 'uploads'   
app = Flask(__name__)  
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
  
@app.route('/', methods=['GET', 'POST'])  
def upload_page():  
    """  
    Renders the upload page and handles the POST request for file uploads.  
    """  
    if request.method == 'POST':  
        # Check if the S3 client is available  
  
        # Check if the POST request has the file part  
        if 'files' not in request.files:  
            return "No file part in the request.", 400
  
        files = request.files.getlist('files')
          
        if not files or files[0].filename == '':  
            return "No files selected for upload.", 400
  
        # Create the temporary upload folder if it doesn't exist  
        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
          
        local_file_paths = []  
        for file in files:  
            # Secure the filename to prevent security issues  
            filename = secure_filename(file.filename)  
            local_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
              
            # Save the file to the temporary local folder  
            file.save(local_path)  
            local_file_paths.append(local_path)
  
        # Use our existing function to upload the files to S3  
        success_count, fail_count, files = begin_upload(local_file_paths)
        
        # --- Cleanup: Remove the temporary local files after upload ---  
        for path in local_file_paths:  
            try:  
              os.remove(path)  
            except OSError as e:  
              print(f"Error removing file {path}: {e}")           
        
        file_list_html = "No files found or error listing files." 
        if files:  
            # Join the list items with line breaks for better readability  
            file_list_html = "<br>".join(files)
  
        return f"""
              <h2>Upload Complete</h2><p>{success_count} file(s) uploaded successfully to S3.</p>
               <p>{fail_count} file(s) failed.</p>
               <h3>--- Listing Files in S3 Bucket temp folder---</h3>
               <div>{file_list_html}</div> 
               <br>
               <a href='/'>Upload More</a>
               """
  
    # For a GET request, just render the upload page  
    return render_template('index.html')
  
if __name__ == '__main__':  
    app.run(debug=True)  