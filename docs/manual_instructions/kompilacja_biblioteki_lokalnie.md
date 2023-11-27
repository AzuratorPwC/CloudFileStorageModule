

## Projekt biblioteki Python do obslugi serwisu Storage Account

### Implementacja zostala wykonana przy uzyciu framework-a Wheel ktory na podstawie naszego kodu tworzy paczke z naszym rozwiazaniem 
Uzyto instrukcji na stronie: https://packaging.python.org/en/latest/tutorials/packaging-projects/

Kroki:
1. Utworzenie repozytorium na Azure Devops (StorageAccountPythonLib)
2. Podlaczenie sie do niego za pomoca Vs Code
3. Dodanie wymaganych folderow/plik
    - folder src(kod rozwiazania)
    - plik LICENSE (kto/jak mozna uzywac biblioteki)
    - plik README.md (krotki opis)
    - plik pyproject.toml(techniczny plik definiujacy metadane naszej biblioteki jak wersja,dodatkowe zaleznosci,wlasciciel)
4. do wygenerowania biblioteki(plik .whl) potrzebny jest zainstalowany python na maszynie
5. Otwieramy nowy terminal w Vs Code, wymagane jest zebysmy znajdowali sie na odpowiedniej sciezce (np.C:\Users\astarosta001\repos\BASDATA\StorageAccountPythonLib),
uruchamiamy komende budujaca biblioteke python -m build
6. W naszym lokalnym folderze pojawi sie folder dist ktory zawiera plik .whl