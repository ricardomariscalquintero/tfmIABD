import pygame
import chess
import numpy as np
from tensorflow.keras.models import load_model
from stockfish import Stockfish
import os

# Configuración Pygame
pygame.init()
WIDTH, HEIGHT = 512, 512
SQ_SIZE = WIDTH // 8
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("Juega contra Nakamura")

# Cargar imágenes de piezas
PIECE_IMAGES = {}
PIECE_MAPPING = {
    "r": "R1", "n": "N1", "b": "B1", "q": "Q1", "k": "K1", "p": "P1",
    "R": "R", "N": "N", "B": "B", "Q": "Q", "K": "K", "P": "P"
}
for symbol, name in PIECE_MAPPING.items():
    PIECE_IMAGES[symbol] = pygame.transform.scale(
        pygame.image.load(f"pieces/{name}.png"), (SQ_SIZE, SQ_SIZE))

# Cargar modelo y encoder
MODEL_PATH = "datos_cnn/modelo_blancas"
ENCODER_PATH = "encoder_blancas.npy"
STOCKFISH_PATH = "motor_ejecutable/stockfish"

model = load_model(MODEL_PATH, compile=False)
encoder_classes = np.load(ENCODER_PATH, allow_pickle=True)

# Iniciar Stockfish
stockfish = Stockfish(path=STOCKFISH_PATH, parameters={
    "Threads": 2,
    "Minimum Thinking Time": 100
})
stockfish.set_elo_rating(2800)
stockfish.set_skill_level(20)

# FEN a tensor
def fen_to_tensor(fen):
    piece_to_plane = {
        'P': 0, 'N': 1, 'B': 2, 'R': 3, 'Q': 4, 'K': 5,
        'p': 6, 'n': 7, 'b': 8, 'r': 9, 'q': 10, 'k': 11
    }
    tensor = np.zeros((8, 8, 12), dtype=np.float32)
    fen_board = fen.split(' ')[0]
    rows = fen_board.split('/')
    for i, row in enumerate(rows):
        col = 0
        for char in row:
            if char.isdigit():
                col += int(char)
            elif char in piece_to_plane:
                tensor[i, col, piece_to_plane[char]] = 1
                col += 1
    return tensor

# Inversión visual
def flip_square_name(uci_move):
    col_map = {'a': 'h', 'b': 'g', 'c': 'f', 'd': 'e', 'e': 'd', 'f': 'c', 'g': 'b', 'h': 'a'}
    def flip(sq):
        col, row = sq[0], sq[1]
        return col_map[col] + str(9 - int(row))
    return flip(uci_move[:2]) + flip(uci_move[2:4])

# Jugada IA
def predict_move(fen, board, top_n=10, umbral_cp=20):
    stockfish.set_fen_position(fen)
    eval_info = stockfish.get_evaluation()

    if eval_info["type"] == "mate" and eval_info["value"] is not None and eval_info["value"] <= 3:
        best_uci = stockfish.get_best_move()
        print("¡Mate detectado! Stockfish lo ejecuta:", best_uci)
        return chess.Move.from_uci(best_uci)

    top_moves_info = stockfish.get_top_moves(top_n)
    if not top_moves_info:
        print("No hay jugadas válidas.")
        return np.random.choice(list(board.legal_moves))

    best_eval = top_moves_info[0].get("Centipawn")
    if best_eval is None:
        print("Evaluación no disponible. Stockfish mueve.")
        return chess.Move.from_uci(top_moves_info[0]["Move"])

    candidatas = []
    for move_info in top_moves_info:
        cp = move_info.get("Centipawn")
        if cp is not None and abs(best_eval - cp) <= umbral_cp:
            candidatas.append(move_info)

    if len(candidatas) == 1:
        best_move = chess.Move.from_uci(candidatas[0]["Move"])
        print("Jugada clara. Stockfish la ejecuta:", best_move.uci())
        return best_move

    tensor = fen_to_tensor(fen).reshape(1, 8, 8, 12)
    prediction = model.predict(tensor, verbose=0)[0]

    scored = []
    for move_info in candidatas:
        move_uci = move_info["Move"]
        try:
            idx = np.where(encoder_classes == move_uci)[0][0]
            ia_score = prediction[idx]
        except IndexError:
            ia_score = 0.0
        scored.append((move_uci, ia_score))

    best_uci, best_score = max(scored, key=lambda x: x[1])
    print(f"\nJugada estilo Nakamura: {flip_square_name(best_uci)} (IA score: {best_score:.4f})")
    return chess.Move.from_uci(best_uci)

# Dibujar tablero
def draw_board(board):
    colors = [pygame.Color("white"), pygame.Color("gray")]
    for row in range(8):
        for col in range(8):
            color = colors[(row + col) % 2]
            pygame.draw.rect(screen, color, pygame.Rect(col * SQ_SIZE, row * SQ_SIZE, SQ_SIZE, SQ_SIZE))

    for row in range(8):
        for col in range(8):
            visual_row = row
            visual_col = col
            square = chess.square(7 - visual_col, visual_row)
            piece = board.piece_at(square)
            if piece:
                img = PIECE_IMAGES.get(piece.symbol())
                if img:
                    screen.blit(img, (col * SQ_SIZE, row * SQ_SIZE))

# Juego principal
board = chess.Board()
running = True
selected_square = None
game_over = False

draw_board(board)
pygame.display.flip()

# Primer movimiento del bot (blancas)
ai_move = predict_move(board.fen(), board)
board.push(ai_move)

while running:
    draw_board(board)
    pygame.display.flip()

    if not game_over and board.is_game_over():
        game_over = True
        result = board.result()
        if board.is_checkmate():
            winner = "Blancas" if board.turn == chess.BLACK else "Negras"
            print(f"Jaque mate. Ganan {winner}")
        else:
            print("Tablas")

    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

        elif event.type == pygame.MOUSEBUTTONDOWN and not game_over:
            x, y = pygame.mouse.get_pos()
            col = 7 - (x // SQ_SIZE)
            row = y // SQ_SIZE
            square = chess.square(col, row)

            if selected_square is None:
                piece = board.piece_at(square)
                if piece and piece.color == chess.BLACK:
                    selected_square = square
            else:
                move = chess.Move(selected_square, square)
                if move in board.legal_moves:
                    board.push(move)
                    if not board.is_game_over():
                        ai_move = predict_move(board.fen(), board)
                        board.push(ai_move)
                selected_square = None

pygame.quit()
