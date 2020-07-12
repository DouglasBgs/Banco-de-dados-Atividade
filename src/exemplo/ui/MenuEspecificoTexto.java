package exemplo.ui;

import java.util.Scanner;

abstract public class MenuEspecificoTexto {
    abstract public void adicionar();
    abstract public void editar();
    abstract public void excluir();
    abstract public void listarTodos();
    protected Scanner entrada;

    public MenuEspecificoTexto() {
        entrada = new Scanner(System.in);
    }
}
