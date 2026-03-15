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

def git_command(command: str) -> str:
    if not command.strip().startswith("git "): return "Tylko komendy git."
    try:
        res = subprocess.run(command, shell=True, capture_output=True, text=True)
        return res.stdout if res.returncode == 0 else f"Błąd git:\n{res.stderr}"
    except Exception as e: return f"Błąd: {e}"

def main():
    api_key = os.environ.get("GEMINI_API_KEY")
    initial_task = os.environ.get("INITIAL_TASK")
    
    if not api_key:
        print("Brak GEMINI_API_KEY.")
        return

    client = genai.Client()
    config = types.GenerateContentConfig(
        tools=[read_file, write_file, git_command],
        system_instruction=(
            "Jesteś agentem AI. Twoim zadaniem jest pisanie kodu ETL (Azure/Python). "
            "Zawsze pracuj na nowej gałęzi (git checkout -b). Po edycji zrób commit. "
            "Zawsze zwracaj krótką, techniczną odpowiedź o wykonanych akcjach."
        ),
        temperature=0.1
    )

    chat = client.chats.create(model="gemini-2.5-pro", config=config)

    print("=== Agent uruchomiony ===")
    
    # Automatyczne wykonanie pierwszego zadania
    if initial_task:
        print(f"Wykonywanie zadania początkowego:\n{initial_task}\n")
        response = chat.send_message(initial_task)
        print(f"AI:\n{response.text}\n")

    # Przejście w tryb interaktywny
    while True:
        try:
            user_input = input("Ty: ")
            if user_input.lower() in ['exit', 'quit']: break
            if not user_input.strip(): continue
            response = chat.send_message(user_input)
            print(f"\nAI: {response.text}\n")
        except Exception as e:
            print(f"Błąd: {e}")

if __name__ == "__main__":
    main()