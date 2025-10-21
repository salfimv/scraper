#!/bin/bash
echo "Setting up Bundestag Scraper for Codespaces..."

# Update package list
sudo apt-get update

# Install Chrome and ChromeDriver
echo "Installing Chrome and ChromeDriver..."
sudo apt-get install -y chromium-browser chromium-chromedriver

# Install Python packages
echo "Installing Python packages..."
pip install selenium tqdm

# Create directories
echo "Creating directories..."
mkdir -p 2021/data_links

# Verify installation
echo "Verifying installation..."
which chromium-browser
which chromedriver
python -c "import selenium; print('Selenium version:', selenium.__version__)"

echo "Setup complete! Run: python scraper_codespaces.py"
