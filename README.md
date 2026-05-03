# Java NIO Asynchronous Chat

Wydajny, w pełni asynchroniczny serwer i klient czatu napisany w czystej Javie. Projekt wykorzystuje bibliotekę Java NIO (Non-blocking I/O) oraz architekturę Event Loop, zastępując tradycyjny, blokujący model wielowątkowy.

## Technologie
- Język: Java
- Sieci (NIO): Selector, ServerSocketChannel, SocketChannel, ByteBuffer
- Współbieżność: ExecutorService, FutureTask, synchronizacja bloków kodu

## Główne cechy projektu
- Asynchroniczny serwer: Multipleksowanie połączeń w jednym wątku za pomocą mechanizmu Selector.
- Bezpieczny zapis: Kolejkowanie wysyłanych danych (flaga OP_WRITE) zapobiegające zakleszczeniom przy wolnych połączeniach.
- Optymalizacja TCP: Autorska obsługa pofragmentowanych pakietów sieciowych oraz zastosowanie opcji SO_REUSEADDR eliminującej problemy ze stanem TIME_WAIT.
- Wydajny klient: Zastosowanie nieblokujących kanałów i aktywnego odpytywania z optymalizacją zużycia procesora.
- Testowanie obciążenia: Mechanizm wielowątkowej symulacji klientów oparty o wzorzec Callable.

## Uruchomienie

Aplikacja wymaga pliku konfiguracyjnego ChatTest.txt umieszczonego w katalogu domowym użytkownika. Wartości muszą być oddzielone znakiem tabulacji (\t).

Struktura pliku:
localhost
9999
Asia	50	Dzień dobry	Do widzenia
Adam	20	Szybka wiadomosc 1	Szybka wiadomosc 2

Po utworzeniu pliku, uruchom metodę main w klasie Main.java.

# NioChatServer
