import os
import subprocess
from google import genai
from google.genai import types

active_agents = {}
client = None

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
    try:
        # Brak sztucznych filtrów - użytkownik ręcznie weryfikuje każdą komendę
        res = subprocess.run(command, shell=True, capture_output=True, text=True)
        return res.stdout if res.returncode == 0 else f"Błąd (kod {res.returncode}):\n{res.stderr}"
    except Exception as e: return f"Błąd: {e}"

def delegate_task(target_agent: str, message: str) -> str:
    if target_agent not in active_agents:
        return f"Błąd: Agent '{target_agent}' nie istnieje."
    target_chat = active_agents[target_agent]
    response = target_chat.send_message(message)
    return f"Odpowiedź od {target_agent}: {response.text}"

def request_new_agent(agent_name: str, system_prompt: str) -> str:
    create_agent(agent_name, system_prompt)
    return f"Sukces: Agent '{agent_name}' został utworzony."

def create_agent(name: str, instruction: str):
    global client
    config = types.GenerateContentConfig(
        tools=[read_file, write_file, execute_cli, delegate_task, request_new_agent],
        system_instruction=instruction,
        temperature=0.1
    )
    active_agents[name] = client.chats.create(model="gemini-2.5-pro", config=config)

def main():
    global client
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Brak GEMINI_API_KEY.")
        return

    client = genai.Client()

    base_instructions = (
        "Używasz snake_case"
        "Jesteś autonomicznym inżynierem. Twoim zadaniem jest samodzielne prowadzenie projektu krok po kroku. "
        "Nigdy nie pytaj użytkownika 'co mam zrobić dalej?'. Zamiast tego analizuj stan prosjektu i sam proponuj następny krok, "
        "wywołując konkretne narzędzie (np. stwórz plik, wykonaj skrypt, zainicjuj terraform, sprawdź status git). "
        "Użytkownik działa jedynie jako weryfikator, który akceptuje, modyfikuje lub odrzuca twoje żądania użycia narzędzi. "
        "Zawsze pracuj na nowej gałęzi (git checkout -b) przed modyfikacją kodu. "
        "Zwracaj bardzo krótkie, techniczne odpowiedzi raportujące twój postęp. "
        "Pisz kod i zmienne po angielsku, ale komunikuj się z użytkownikiem po polsku. "
    )

    create_agent(
        "INFRA", 
        base_instructions + "Jesteś agentem DevOps. Twoją domeną jest Terraform i infrastruktura chmurowa Azure. Samodzielnie pisz kod IaC i wykonuj komendy 'terraform init', 'plan', 'apply'. Jeśli potrzebujesz wiedzy o strukturze danych, deleguj to do agenta ETL."
    )
    
    create_agent(
        "ETL", 
        base_instructions + "Jesteś agentem Data Engineer. Piszesz kod w Python/PySpark do przetwarzania w architekturze Medallion. Samodzielnie analizuj dane źródłowe (przez skrypty próbne) i buduj pipeline. Jeśli potrzebujesz zasobów chmurowych, deleguj to do agenta INFRA."
    )

    current_agent = "INFRA"

    print("\n=== SYSTEM AUTONOMICZNY ===")
    print("Wpisz inicjalny cel. Od tego momentu system sam proponuje działania.")
    
    while True:
        try:
            print(f"\n[{current_agent}] Oczekuje...")
            user_input = input("Ty (t - kontynuuj / m - modyfikuj instrukcję / switch [nazwa] / exit): ").strip()
            
            if user_input.lower() in ['exit', 'quit']: break
            if not user_input: continue
            
            if user_input.startswith("switch "):
                target = user_input.split(" ")[1].upper()
                if target in active_agents:
                    current_agent = target
                    print(f"Przełączono na {current_agent}")
                else:
                    print("Błąd: Taki agent nie istnieje.")
                continue

            # Translacja intencji na polecenia dla modelu
            if user_input.lower() == 't':
                user_input = "Zgoda, to poprawny krok. Kontynuuj swój plan i zaproponuj/wykonaj następną logiczną akcję."
            elif user_input.lower() == 'n':
                user_input = "Nie, zatrzymaj to podejście. Przemyśl problem jeszcze raz i zaproponuj inne rozwiązanie."
            
            chat = active_agents[current_agent]
            response = chat.send_message(user_input)
            
            # Weryfikacja proponowanych akcji (Human-in-the-Loop)
            while response.function_calls:
                function_responses = []
                for tool_call in response.function_calls:
                    name = tool_call.name
                    args = tool_call.args
                    
                    print(f"\n⚠️  PROPOZYCJA AKCJI: {name}")
                    print("📦 Argumenty:")
                    for key, val in args.items():
                        if isinstance(val, str) and '\n' in val:
                            print(f"\n--- Początek: {key} ---")
                            print(val)
                            print(f"--- Koniec: {key} ---\n")
                        else:
                            print(f"  {key}: {val}")
                    
                    decision = input("\nZatwierdzasz wykonanie na swoim dysku? (t - tak / n - nie / m - modyfikuj): ").strip().lower()
                    
                    if decision == 'n':
                        res = "SYSTEM: Użytkownik zablokował wywołanie tego narzędzia. Zaproponuj inny krok bez używania tej konkretnej komendy."
                    elif decision == 'm':
                        hint = input("Wpisz co ma zostać zmienione: ")
                        res = f"SYSTEM: Użytkownik zablokował akcję i przekazał wskazówkę: '{hint}'. Dostosuj się do niej przed kolejnym krokiem."
                    else:
                        print("Wykonywanie...")
                        if name == "read_file": res = read_file(**args)
                        elif name == "write_file": res = write_file(**args)
                        elif name == "execute_cli": res = execute_cli(**args)
                        elif name == "delegate_task": res = delegate_task(**args)
                        elif name == "request_new_agent": res = request_new_agent(**args)
                        else: res = f"Nieznane narzędzie: {name}"
                    
                    function_responses.append(
                        types.Part.from_function_response(name=name, response={"result": res})
                    )
                response = chat.send_message(function_responses)

            if response.text:
                print(f"\n{current_agent}: {response.text}\n")
                
        except Exception as e:
            print(f"Błąd wykonania pętli: {e}")

if __name__ == "__main__":
    main()