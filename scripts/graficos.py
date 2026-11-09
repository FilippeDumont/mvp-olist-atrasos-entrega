"""
graficos.py — padrão visual único dos gráficos do projeto (matplotlib).

Princípios:
  * paleta categórica fixa, em ordem fixa, validada para daltonismo
    (azul, laranja, verde-água; cinza para elementos de contexto);
  * marcas finas, grade discreta, sem gráficos de dois eixos y;
  * rótulos diretos apenas nos valores que importam;
  * cada figura é salva em PNG (200 dpi) na pasta de evidências, no Drive e no repositório.
"""
import os
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

AZUL, LARANJA, AQUA, CINZA = "#2a78d6", "#eb6834", "#1baf7a", "#b4b2a9"
TEXTO, TEXTO_2, GRADE = "#0b0b0b", "#52514e", "#e6e4dd"
AZUL_RAMPA = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]

matplotlib.rcParams.update({
    "figure.dpi": 110, "savefig.dpi": 200, "savefig.bbox": "tight",
    "figure.facecolor": "white", "axes.facecolor": "white",
    "font.size": 10, "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "axes.titlepad": 24, "axes.labelsize": 10, "axes.labelcolor": TEXTO_2,
    "axes.edgecolor": GRADE, "axes.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "axes.grid.axis": "y", "grid.color": GRADE, "grid.linewidth": 0.6,
    "xtick.color": TEXTO_2, "ytick.color": TEXTO_2, "xtick.labelsize": 9, "ytick.labelsize": 9,
    "legend.frameon": False, "legend.fontsize": 9, "text.color": TEXTO,
    "lines.linewidth": 2, "lines.markersize": 5,
})


def br(v, casas=1):
    """Número no padrão brasileiro: vírgula decimal e ponto de milhar."""
    return f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def pct_axis(ax, axis="y", decimals=0):
    fmt = mtick.FuncFormatter(lambda v, _: f"{br(v, decimals)}%")
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(fmt)


def subtitulo(ax, texto):
    """Subtítulo explicativo logo abaixo do título."""
    ax.annotate(texto, xy=(0, 1), xycoords="axes fraction", xytext=(0, 5), textcoords="offset points",
                fontsize=9, color=TEXTO_2, va="bottom")


def fonte(fig, texto="Fonte: Olist (Kaggle), compras de jan/2017 a ago/2018, pedidos entregues. Elaboração própria."):
    """Nota de fonte posicionada abaixo de TODOS os elementos já desenhados (eixos, rótulos, legendas).
    Deve ser a última chamada antes de salvar()."""
    fig.canvas.draw()
    bb = fig.get_tightbbox(fig.canvas.get_renderer())
    x, y = bb.x0 / fig.get_figwidth(), bb.y0 / fig.get_figheight()
    fig.text(x, y - 0.012, texto, fontsize=7.5, color=TEXTO_2, ha="left", va="top")


def salvar(fig, nome, destinos):
    """Salva a figura em todas as pastas de destino (Drive e repositório)."""
    for d in destinos:
        os.makedirs(d, exist_ok=True)
        fig.savefig(os.path.join(d, f"{nome}.png"))
    plt.show()
    plt.close(fig)
