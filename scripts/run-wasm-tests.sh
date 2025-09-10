#!/usr/bin/env bash

# Script to run WASM tests for Ankurah project
# Usage: ./scripts/run-wasm-tests.sh [package_name|--discover]
# If no package name is provided, runs tests for all discovered wasm packages
# Use --discover to only show discovered packages without running tests

set -e

# Check if we're running with bash
if [ -z "$BASH_VERSION" ]; then
    echo "This script requires bash. Please run with: bash $0"
    exit 1
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if wasm-pack is installed
check_wasm_pack() {
    if ! command -v wasm-pack &> /dev/null; then
        print_error "wasm-pack is not installed. Please install it first:"
        print_error "  curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh"
        exit 1
    fi
}

# Function to automatically discover wasm test packages
discover_wasm_packages() {
    local discovered_packages=""
    
    # Find all Cargo.toml files in the workspace (excluding target directories and derive test directories)
    while IFS= read -r -d '' cargo_toml; do
        # Skip if it's in a target directory or derive test directory
        if [[ "$cargo_toml" == *"/target/"* ]] || [[ "$cargo_toml" == *"/derive/tests/"* ]]; then
            continue
        fi
        
        # Check if this Cargo.toml has wasm-bindgen-test in dev-dependencies
        if grep -q "wasm-bindgen-test" "$cargo_toml" 2>/dev/null; then
            # Get the directory containing this Cargo.toml
            local package_dir=$(dirname "$cargo_toml")
            # Get relative path from project root
            local rel_path=${package_dir#"$PROJECT_ROOT/"}
            
            # Extract package name from Cargo.toml
            local package_name=$(grep '^name.*=' "$cargo_toml" | head -1 | sed 's/.*=.*"\([^"]*\)".*/\1/' | sed 's/ankurah-//' | sed 's/-/_/g')
            
            # Only include packages that look like real wasm packages (not test fixtures)
            if [ -n "$package_name" ] && [ "$rel_path" != "." ] && [[ "$package_name" == *"wasm"* ]]; then
                discovered_packages="$discovered_packages
        $package_name:$rel_path"
            fi
        fi
    done < <(find "$PROJECT_ROOT" -name "Cargo.toml" -print0)
    
    echo "$discovered_packages"
}

# Function to run wasm tests for a specific package
run_wasm_tests() {
    local package_dir=$1
    local package_name=$2
    
    if [ ! -d "$package_dir" ]; then
        print_warning "Package directory $package_dir does not exist, skipping"
        return 0
    fi
    
    print_status "Running WASM tests for $package_name..."
    
    cd "$package_dir"
    
    # Check if the package has wasm tests
    if [ ! -f "Cargo.toml" ]; then
        print_warning "No Cargo.toml found in $package_dir, skipping"
        cd - > /dev/null
        return 0
    fi
    
    # Check if wasm-bindgen-test is in dev-dependencies
    if ! grep -q "wasm-bindgen-test" Cargo.toml; then
        print_warning "No wasm-bindgen-test found in $package_name, skipping"
        cd - > /dev/null
        return 0
    fi
    
    # Run the tests
    if wasm-pack test --headless --chrome; then
        print_status "✅ WASM tests passed for $package_name"
    else
        print_error "❌ WASM tests failed for $package_name"
        cd - > /dev/null
        exit 1
    fi
    
    cd - > /dev/null
}

# Main execution
main() {
    local target_package="$1"
    
    # Get the project root directory (where this script is located)
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
    
    cd "$PROJECT_ROOT"
    
    print_status "Starting WASM tests from project root: $PROJECT_ROOT"
    
    # Check dependencies
    check_wasm_pack
    
    # Auto-discover wasm packages or use manually defined ones
    wasm_packages=$(discover_wasm_packages)
    
    # Fallback to manually defined packages if auto-discovery fails
    if [ -z "$wasm_packages" ]; then
        print_warning "Auto-discovery found no packages, using manual list..."
        wasm_packages="
        indexeddb-wasm:storage/indexeddb-wasm
        websocket-client-wasm:connectors/websocket-client-wasm
    "
    fi
    
    # If --discover flag is used, just show discovered packages and exit
    if [ "$target_package" = "--discover" ]; then
        print_status "Discovered WASM test packages:"
        if [ -n "$wasm_packages" ]; then
            while IFS=':' read -r package_name package_dir; do
                [ -z "$package_name" ] && continue
                echo "  - $package_name -> $package_dir"
            done <<< "$wasm_packages"
        else
            print_warning "No WASM test packages found"
        fi
        exit 0
    fi
    
    if [ -n "$target_package" ]; then
        # Run tests for specific package
        found=false
        while IFS=':' read -r package_name package_dir; do
            # Skip empty lines
            [ -z "$package_name" ] && continue
            
            if [ "$package_name" = "$target_package" ]; then
                run_wasm_tests "$package_dir" "$package_name"
                found=true
                break
            fi
        done <<< "$wasm_packages"
        
        if [ "$found" = false ]; then
            print_error "Unknown wasm package: $target_package"
            print_error "Available packages:"
            while IFS=':' read -r package_name package_dir; do
                [ -z "$package_name" ] && continue
                print_error "  - $package_name"
            done <<< "$wasm_packages"
            exit 1
        fi
    else
        # Run tests for all wasm packages
        print_status "Running tests for all WASM packages..."
        
        while IFS=':' read -r package_name package_dir; do
            # Skip empty lines
            [ -z "$package_name" ] && continue
            run_wasm_tests "$package_dir" "$package_name"
        done <<< "$wasm_packages"
    fi
    
    print_status "🎉 All WASM tests completed successfully!"
}

# Run main function with all arguments
main "$@"
