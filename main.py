#!/usr/bin/env python3
from __future__ import annotations
import argparse, sys, traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.append(str(ROOT / "src"))

def run_eda():
    from exploracao import main as eda_main
    eda_main()

def run_testes():
    from testes import main as testes_main
    testes_main()

def run_previsao():
    from previsao import main as prev_main
    prev_main()

def parse_args():
    p = argparse.ArgumentParser(description="Orquestrador do pipeline de séries temporais (EDA, Testes, Previsão).")
    p.add_argument("--eda", action="store_true", help="Executa apenas a exploração de dados")
    p.add_argument("--testes", action="store_true", help="Executa apenas os testes estatísticos")
    p.add_argument("--previsao", action="store_true", help="Executa apenas a previsão (treino+forecast)")
    return p.parse_args()

def main():
    args = parse_args()
    stages = []
    if not (args.eda or args.testes or args.previsao):
        stages = [run_eda, run_testes, run_previsao]
    else:
        if args.eda: stages.append(run_eda)
        if args.testes: stages.append(run_testes)
        if args.previsao: stages.append(run_previsao)

    for stage in stages:
        name = stage.__name__
        print(f"\n===== Iniciando etapa: {name} =====\n")
        try:
            stage()
            print(f"\n>>> Etapa concluída: {name}\n")
        except Exception as e:
            print(f"\n!!! ERRO na etapa {name}: {e}\n", file=sys.stderr)
            traceback.print_exc()
            sys.exit(1)

    print("\nPipeline finalizado com sucesso. Artefatos em 'outputs/'.\n")

if __name__ == "__main__":
    main()
