import os
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
import requests

load_dotenv(override=True)

# Environment variables
ACCOUNT = os.getenv("ACCOUNT")
HOST = os.getenv("HOST")
USER = os.getenv("USER")
DATABASE = os.getenv("DATABASE")
SCHEMA = os.getenv("SCHEMA")
ROLE = os.getenv("ROLE")
WAREHOUSE = os.getenv("WAREHOUSE")
AGENT_ENDPOINT = os.getenv("AGENT_ENDPOINT")
SEMANTIC_MODEL = os.getenv("SEMANTIC_MODEL")
RSA_PRIVATE_KEY_PATH = os.getenv("RSA_PRIVATE_KEY_PATH")
RSA_PRIVATE_KEY_PASSWORD = os.getenv("RSA_PRIVATE_KEY_PASSWORD")
MODEL = os.getenv("MODEL")

def test_configuration():
    """Test Snowflake configuration step by step"""
    
    print("=== SNOWFLAKE CONFIGURATION TEST ===\n")
    
    # Test 1: Environment Variables
    print("1. TESTING ENVIRONMENT VARIABLES:")
    required_vars = {
        'ACCOUNT': ACCOUNT,
        'HOST': HOST, 
        'USER': USER,
        'DATABASE': DATABASE,
        'SCHEMA': SCHEMA,
        'ROLE': ROLE,
        'WAREHOUSE': WAREHOUSE,
        'AGENT_ENDPOINT': AGENT_ENDPOINT,
        'SEMANTIC_MODEL': SEMANTIC_MODEL,
        'RSA_PRIVATE_KEY_PATH': RSA_PRIVATE_KEY_PATH,
        'MODEL': MODEL
    }
    
    for var_name, var_value in required_vars.items():
        if var_value:
            print(f"   ✓ {var_name}: {var_value}")
        else:
            print(f"   ✗ {var_name}: MISSING!")
            
    print()
    
    # Test 2: Private Key File
    print("2. TESTING PRIVATE KEY FILE:")
    try:
        with open(RSA_PRIVATE_KEY_PATH, "rb") as pem_in:
            pemlines = pem_in.read()
        private_key_obj = load_pem_private_key(
            pemlines, 
            password=RSA_PRIVATE_KEY_PASSWORD.encode() if RSA_PRIVATE_KEY_PASSWORD else None, 
            backend=default_backend()
        )
        print(f"   ✓ Private key loaded successfully from {RSA_PRIVATE_KEY_PATH}")
    except Exception as e:
        print(f"   ✗ Error loading private key: {e}")
        return False
    
    print()
    
    # Test 3: Database Connection
    print("3. TESTING DATABASE CONNECTION:")
    try:
        conn = snowflake.connector.connect(
            user=USER,
            account=ACCOUNT,
            private_key=private_key_obj,
            warehouse=WAREHOUSE,
            role=ROLE,
            host=HOST,
            database=DATABASE,
            schema=SCHEMA
        )
        print("   ✓ Database connection successful!")
        
        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        print(f"   ✓ Snowflake version: {version}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   ✗ Database connection failed: {e}")
        return False
    
    print()
    
    # Test 4: Semantic Model File Access
    print("4. TESTING SEMANTIC MODEL ACCESS:")
    try:
        # Try to connect and check if the semantic model file exists
        conn = snowflake.connector.connect(
            user=USER,
            account=ACCOUNT,
            private_key=private_key_obj,
            warehouse=WAREHOUSE,
            role=ROLE,
            host=HOST,
            database=DATABASE,
            schema=SCHEMA
        )
        
        cursor = conn.cursor()
        
        # Parse the semantic model path to check if the stage exists
        if SEMANTIC_MODEL.startswith('@'):
            stage_path = SEMANTIC_MODEL.split('/')[0]  # Get just the stage part
            try:
                cursor.execute(f"LIST {stage_path}")
                files = cursor.fetchall()
                print(f"   ✓ Stage {stage_path} accessible with {len(files)} files")
                
                # Check if the specific file exists
                file_found = any(SEMANTIC_MODEL.split('/')[-1] in str(file) for file in files)
                if file_found:
                    print(f"   ✓ Semantic model file found: {SEMANTIC_MODEL}")
                else:
                    print(f"   ⚠ Semantic model file not found in stage: {SEMANTIC_MODEL}")
                    print("   Available files:")
                    for file in files[:5]:  # Show first 5 files
                        print(f"     - {file}")
                        
            except Exception as e:
                print(f"   ✗ Error accessing stage {stage_path}: {e}")
                
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   ✗ Error checking semantic model: {e}")
    
    print()
    
    # Test 5: Agent Endpoint Format
    print("5. TESTING AGENT ENDPOINT FORMAT:")
    if AGENT_ENDPOINT:
        if '/api/v2/cortex/agent:run' in AGENT_ENDPOINT:
            print(f"   ✓ Agent endpoint format looks correct: {AGENT_ENDPOINT}")
        else:
            print(f"   ⚠ Agent endpoint might be incorrect: {AGENT_ENDPOINT}")
            print("   Expected format: https://<account>.snowflakecomputing.com/api/v2/cortex/agent:run")
    else:
        print("   ✗ Agent endpoint is missing!")
    
    print()
    
    # Test 6: Model Name
    print("6. TESTING MODEL NAME:")
    valid_models = [
        'llama3.1-8b', 'llama3.1-70b', 'llama3.1-405b',
        'llama3.2-1b', 'llama3.2-3b',
        'llama3.3-70b',
        'mistral-large2', 'mistral-7b',
        'mixtral-8x7b',
        'gemma-7b'
    ]
    
    if MODEL in valid_models:
        print(f"   ✓ Model name is valid: {MODEL}")
    else:
        print(f"   ⚠ Model name might be invalid: {MODEL}")
        print(f"   Valid models: {', '.join(valid_models)}")
    
    print()
    
    # Test 7: Cortex Analyst/Agents Availability
    print("7. TESTING CORTEX ANALYST/AGENTS AVAILABILITY:")
    try:
        conn = snowflake.connector.connect(
            user=USER,
            account=ACCOUNT,
            private_key=private_key_obj,
            warehouse=WAREHOUSE,
            role=ROLE,
            host=HOST,
            database=DATABASE,
            schema=SCHEMA
        )
        
        cursor = conn.cursor()
        
        # Check if we can access Cortex Analyst functions
        try:
            # This is a basic test - Cortex Analyst doesn't have a simple test function
            # But we can check if the role has the necessary privileges
            cursor.execute("SHOW GRANTS TO ROLE " + ROLE)
            grants = cursor.fetchall()
            
            cortex_privileges = [grant for grant in grants if 'CORTEX' in str(grant).upper()]
            if cortex_privileges:
                print("   ✓ Found Cortex-related privileges for role:")
                for grant in cortex_privileges:
                    print(f"     - {grant}")
            else:
                print("   ⚠ No explicit Cortex privileges found - this might be okay if inherited")
                
        except Exception as e:
            print(f"   ⚠ Could not check role privileges: {e}")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   ✗ Error testing Cortex access: {e}")
    
    print()
    
    # Test 8: Account Region and Cortex Agents Support
    print("8. TESTING ACCOUNT REGION:")
    if ACCOUNT:
        if any(region in ACCOUNT.lower() for region in ['us-east-1', 'us-west-2', 'eu-west-1']):
            print(f"   ✓ Account appears to be in a major region: {ACCOUNT}")
        else:
            print(f"   ⚠ Account region might not support Cortex Agents yet: {ACCOUNT}")
            print("   Cortex Agents is still rolling out to different regions")
    else:
        print("   ✗ Account identifier is missing!")
    
    print("\n=== CONFIGURATION TEST COMPLETE ===")
    return True

if __name__ == "__main__":
    test_configuration()