import os
from bs4 import BeautifulSoup

TARGET_DIR = "/home/chan/code/wiki/gwiki_output_big/wiki/A/"
HEADER_ID = "gwiki-custom-header" # Used to prevent duplicate headers

# --- Define the HTML for the header ---
# The paths for logo and links are relative to how they'd be served.
# e.g., /wiki/assets/logo.png assumes your wiki is served at example.com/wiki/
# and you have an 'assets' folder in your wiki's static root.
# The 'Home' link goes to the site root '/'.
# The 'About This Subset' link goes to 'not_g.html' at the root of the wiki directory.

HEADER_HTML = f"""
<div id="{HEADER_ID}">
    <div class="gwiki-header-section gwiki-branding">
        <img src="/wiki/assets/logo.png" alt="Site Logo" id="gwiki-logo" />
        <span id="gwiki-site-title">G-Wiki Subset</span>
    </div>
    <div class="gwiki-header-section gwiki-navigation">
        <input type="search" id="gwiki-searchbar" placeholder="Search G-articles..." aria-label="Search articles" />
        <a href="/" class="gwiki-nav-link">Home</a>
        <a href="../not_g.html" class="gwiki-nav-link">About This Subset</a>
    </div>
</div>
"""

# --- Suggested CSS for the header (to be added to your site's stylesheet) ---
SUGGESTED_CSS = """
/* Suggested CSS for the injected header */
/* You should place this in a global CSS file for your wiki,
   e.g., /wiki/-/custom-styles.css and link it in your template or pages */

#gwiki-custom-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 10px 20px;
    background-color: #f8f9fa; /* Light grey background */
    border-bottom: 1px solid #dee2e6; /* Subtle border */
    font-family: sans-serif; /* Basic font */
    margin-bottom: 15px; /* Space below header */
}

.gwiki-header-section {
    display: flex;
    align-items: center;
}

#gwiki-logo {
    height: 40px; /* Adjust as needed */
    margin-right: 15px;
}

#gwiki-site-title {
    font-size: 1.5em; /* Adjust as needed */
    font-weight: bold;
    color: #333;
}

#gwiki-searchbar {
    padding: 8px 12px;
    border: 1px solid #ccc;
    border-radius: 4px;
    margin-right: 20px; /* Space between search and links */
    min-width: 250px; /* Adjust as needed */
}

.gwiki-nav-link {
    margin-left: 15px; /* Space between nav links */
    text-decoration: none;
    color: #007bff; /* Bootstrap blue, adjust as needed */
    font-size: 1em;
}

.gwiki-nav-link:hover {
    text-decoration: underline;
    color: #0056b3;
}

/* Basic responsive consideration: stack elements on smaller screens */
@media (max-width: 768px) {
    #gwiki-custom-header {
        flex-direction: column;
        align-items: flex-start;
    }
    .gwiki-navigation {
        margin-top: 10px;
        width: 100%;
        display: flex;
        flex-direction: column;
        align-items: flex-start;
    }
    #gwiki-searchbar {
        margin-right: 0;
        margin-bottom: 10px;
        width: calc(100% - 24px); /* Adjust for padding */
    }
    .gwiki-nav-link {
        margin-left: 0;
        margin-bottom: 5px;
    }
}
"""

def inject_header_into_html_file(filepath):
    """
    Reads an HTML file, injects the header if not present, and saves it.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        soup = BeautifulSoup(content, 'html.parser') # Using html.parser for broader compatibility

        # Check if header already exists
        if soup.find(id=HEADER_ID):
            # print(f"Header already exists in {filepath}, skipping.")
            return False # No change made

        body_tag = soup.body
        if not body_tag:
            print(f"Warning: No <body> tag found in {filepath}, skipping header injection.")
            return False # No change made

        # Create the header element from the HTML string
        header_soup = BeautifulSoup(HEADER_HTML, 'html.parser')
        # The above parsing will likely wrap HEADER_HTML in <html><body> tags,
        # so we need to extract the actual div.
        new_header_div = header_soup.find('div', id=HEADER_ID)

        if new_header_div:
            body_tag.insert(0, new_header_div) # Insert header at the beginning of the body
        else:
            print(f"Error: Could not properly create header element for {filepath}.")
            return False

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(str(soup))
        # print(f"Header injected into {filepath}")
        return True # Change made

    except Exception as e:
        print(f"Error processing file {filepath}: {e}")
        return False

def main():
    print("--- Starting Header Injection Script ---")
    print(f"WARNING: This script will modify HTML files in-place in '{TARGET_DIR}'.")
    print("PLEASE ENSURE YOU HAVE A BACKUP OF THIS DIRECTORY BEFORE PROCEEDING.")
    # Python 3.x input
    proceed = input("Do you want to continue? (yes/no): ")
    if proceed.lower() != 'yes':
        print("Operation cancelled by user.")
        return

    if not os.path.isdir(TARGET_DIR):
        print(f"Error: Target directory '{TARGET_DIR}' not found.")
        return

    print(f"Processing HTML files in '{TARGET_DIR}'...")
    files_processed = 0
    files_changed = 0

    for filename in os.listdir(TARGET_DIR):
        if filename.lower().endswith(".html"):
            filepath = os.path.join(TARGET_DIR, filename)
            if os.path.isfile(filepath): # Ensure it's a file
                files_processed += 1
                if inject_header_into_html_file(filepath):
                    files_changed += 1
                if files_processed % 100 == 0:
                    print(f"  ... processed {files_processed} files ({files_changed} modified so far) ...")


    print("\n--- Header Injection Complete ---")
    print(f"Total HTML files found and processed: {files_processed}")
    print(f"HTML files modified with new header: {files_changed}")

    print("\n--- Suggested CSS for the Header ---")
    print("Please add the following CSS to your website's stylesheet to style the new header:")
    print("You might need to adjust paths (e.g., for the logo) and styles to fit your site.")
    print("------------------------------------")
    print(SUGGESTED_CSS)
    print("------------------------------------")
    print("Remember to create the logo image at '/wiki/assets/logo.png' (or update the path in the script).")
    print("The search bar is a static HTML element; search functionality needs separate implementation (e.g., JavaScript).")

if __name__ == "__main__":
    main()
