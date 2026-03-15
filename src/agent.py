import os
import subprocess
from google import genai
from google.genai import types

def read_file(filepath: str) -> str:
    try:
        with open(filepath, 'r', encoding='utf-8') as f: return f.read()
    except Exception as e: return f"Błąd: {e}"

def write_file(filepath: str, content: str) -> str:
    try:
        os.makedirs(os.path.dirname(filepath) or '.', exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f: f.write(content)
        return f"Zapisano: {filepath}"
    except Exception as e: return f"Błąd: {e}"

def execute_cli(command: str) -> str:
    """Wykonuje dozwolone komendy w terminalu kontenera."""
    allowed_prefixes = ("git ", "terraform ", "az ")
    if not command.strip().startswith(allowed_prefixes): 
        return "Błąd: Zezwolono tylko na polecenia zaczynające się od 'git', 'terraform' lub 'az'."
    try:
        res = subprocess.run(command, shell=True, capture_output=True, text=True)
        return res.stdout if res.returncode == 0 else f"Błąd (kod {res.returncode}):\n{res.stderr}"
    except Exception as e: return f"Błąd krytyczny wykonania: {e}"

def main():
    api_key = os.environ.get("GEMINI_API_KEY")
    agent_role = os.environ.get("AGENT_ROLE", "ETL")
    
    if not api_key:
        print("Brak GEMINI_API_KEY.")
        return

    # Logika zmiany osobowości w zależności od kontenera
    system_prompt = (
        "Zawsze pracuj na nowej gałęzi (git checkout -b) przed modyfikacją kodu. "
        "Zawsze zwracaj krótką, techniczną odpowiedź o wykonanych akcjach." 
        "Przed podjęciem pracy upewnij się że dostałeś odpowiednie instrukcje."
        "Jeśli nie jesteś pewien i nie masz wystarczających informacji, zapytaj i poproś o więcej szczegółów. Nie zakładaj i nie halucynuj, że wiesz co robić."
        "Nie halucynuj w kodzie i próbuj zawsze używać istniejących funkcji" 
        "Pisz rozwiązanie po angielku, ale komunikuj się ze mną po polsku."
    )
    if agent_role == "INFRA":
        system_prompt += (
            "Jesteś agentem DevOps i Cloud Architektem. Odpowiadasz za infrastrukturę na Azure. "
            "Tworzysz kod w Terraform (*.tf). Masz dostęp do narzędzi 'az' (Azure CLI) oraz 'terraform'. "
        )
        print("=== Agent INFRASTRUKTURY uruchomiony ===")
    else:
        system_prompt += (
            "Jesteś agentem Data Engineer. Piszesz kod ETL w Pythonie/PySpark. "
        )
        print("=== Agent ETL uruchomiony ===")

    client = genai.Client()
    config = types.GenerateContentConfig(
        tools=[read_file, write_file, execute_cli],
        system_instruction=system_prompt,
        temperature=0.1
    )

    chat = client.chats.create(model="gemini-2.5-pro", config=config)

    while True:
        try:
            user_input = input("Ty: ")
            if user_input.lower() in ['exit', 'quit']: break
            if not user_input.strip(): continue
            
            response = chat.send_message(user_input)
            
            while response.function_calls:
                function_responses = []
                for tool_call in response.function_calls:
                    name = tool_call.name
                    args = tool_call.args
                    print(f"\n🛠️  Narzędzie: {name} | Arg: {args}")
                    
                    if name == "read_file": res = read_file(**args)
                    elif name == "write_file": res = write_file(**args)
                    elif name == "execute_cli": res = execute_cli(**args)
                    else: res = f"Nieznane narzędzie: {name}"
                    
                    function_responses.append(
                        types.Part.from_function_response(name=name, response={"result": res})
                    )
                response = chat.send_message(function_responses)

            if response.text:
                print(f"\nAI: {response.text}\n")
                
        except Exception as e:
            print(f"Błąd wykonania: {e}")

if __name__ == "__main__":
    main()