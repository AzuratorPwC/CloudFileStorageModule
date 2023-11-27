### Instrukcja jak skonfigurowac narzedzie pip w celu sciagniecia biblioteki z naszej biblioteki na AzureDevOps

1.  Wchodzimy na Azure Devops 
2.  Wchodzimy w Personal access tokens <br />
        ![azure_init](images/token1.JPG)
3.  Klikamy New Token i zaznaczamy opcje jak na obrazku,na boku zapisujemy wygenerowany token
        ![azure_init](images/token2.JPG)
4.  Tworzymy folder w sciezce C:\ProgramData\Pip
5.  Tworzymy plik pip.ini <br />
    ![azure_init](images/pipini.JPG)
6.  Sciezke do repozytoriow znajdujemy w Azure DevOps w Artifacts:
    - klikamy [Artifacts]
    - wybieramy repozytrium w lewej czesci [PwCDataLibs]
    - klikamy [Connect to Feed]
    ![azure_init](images/artifact1.JPG)

7.  Wybieramy Pip(dotyczy to paczek Python sciaganych za pomoca polecenia pip)<br />
    ![azure_init](images/artifact2.JPG)

8.  Po otwarciu widzimy sciezke do naszego repozytorium <br />
    ![azure_init](images/artifact3.JPG)
9.  Kopiujemy adres do pliku jednak z pewnymi modyfikacjami
    - dodajemy nasz mail
    - dodajemy wygenerowany token z punktu 3
    - "index-url" zamieniamy na "extra-index-url"

    [global] <br />
    extra-index-url= <br />
    https://[uzytkownik z maila]%40pwc.com:[token]@pkgs.dev.azure.com/BASDATA/      bd61f679-2c80-40cb-9e82-9e6bea9f511f/_packaging/PwCDataLibs/pypi/simple/

    na przyklad:<br />
    
    ```
    [global]
        extra-index-url= 
            https://artur.starosta%40pwc.com:44444ggggghh@pkgs.dev.azure.com/BASDATA/bd61f679-2c80-40cb-9e82-9e6bea9f511f/_packaging/PwCDataLibs/pypi/simple/
    ```
    Uwaga! <br />
    kolejne external linki sa dodawana jako kolejne linie(w przypadku tego rozwiazanie powiny byc dwa wpisy,jedno dla produkcji drugie do developerskiego repozytrium) <br />
    Zawartosc pliku pip.ini <br />
    ![azure_init](images/artifact4.JPG)

10. Sciaganie bibliotek z dodanych repozytoriow na lokalny 
    komputer, najlepiej zrobic poprzez srodowisko wirtualne python,unikmiemy wtedy konfliktow z roznymi wersjami bibliotek ktore juz mamy
    https://docs.python.org/3/tutorial/venv.html <br />

    Po uruchmieniu srodowiska wirtualnego powinnismy widziec w naszym cmd nazwe srodowiska<br />
    ![azure_init](images/pythonvenv.JPG)

    Sciagamy biblioteki z repozytiow: 
    ```
    pip install cloud-storage-pwc-dev
    pip install cloud-storage-pwc
    ```
    Zainstalowanie konkretnej wersji biblioteki
    ```
    pip install cloud-storage-pwc-dev==0.5
    ```
