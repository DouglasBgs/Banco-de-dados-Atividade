package exemplo;

import java.util.List;

import exemplo.dao.DatabaseAccess;
import exemplo.dao.DepartamentoDao;
import exemplo.dao.PessoaDao;
import exemplo.modelo.Departamento;
import exemplo.modelo.Pessoa;
import exemplo.ui.MenuPrincipalTexto;

public class Principal {

	public static void main(String[] args) {
		/*
		 * // a tabela departamento é criada ao instanciar a classe. Se já existir, //
		 * nada acontece DepartamentoDao dDao = new DepartamentoDao(); PessoaDao pDao =
		 * new PessoaDao();
		 * 
		 * // Inserir deptos Departamento d1 = new Departamento(0, "Administrativo",
		 * null); System.out.println("Inserindo departamento 1: " + d1);
		 * dDao.insereDepartamento(d1);
		 * 
		 * Departamento d2 = new Departamento(0, "Desenvolvimento", null);
		 * System.out.println("Inserindo departamento 2: " + d2);
		 * dDao.insereDepartamento(d2);
		 * 
		 * // seleciona todos os departamentos List<Departamento> deptos =
		 * dDao.getAllDepartamentos(); imprimeDepartamentos(deptos);
		 * 
		 * 
		 * // Inserir pessoas Pessoa p1 = new Pessoa(1, "Maria", 50);
		 * System.out.println("Inserindo Pessoa 1: " + p1); pDao.inserePessoa(p1);
		 * 
		 * Pessoa p2 = new Pessoa(2, "José", 36);
		 * System.out.println("Inserindo Pessoa 2: " + p2); pDao.inserePessoa(p2);
		 * 
		 * 
		 * // faz um SELECT no banco---------------------------------- List<Pessoa>
		 * pessoas = pDao.getAllPessoas();
		 * 
		 * imprimePessoas(pessoas);
		 * 
		 * // ----------------------------------------- // atualiza pessoa
		 * p1.setNome("Mariano"); p1.setIdade(20);
		 * 
		 * pDao.updatePessoa(p1);
		 * 
		 * // remove pessoa ---------------------------------------
		 * System.out.println("Removendo pessoa 2"); pDao.deletePessoa(p2.getId());
		 * 
		 * // lista todas as pessoas pessoas = pDao.getAllPessoas();
		 * 
		 * imprimePessoas(pessoas);
		 */
		
		MenuPrincipalTexto menu = new MenuPrincipalTexto();
		
		menu.executa();
	}

	private static void imprimePessoas(List<Pessoa> pessoas) {
		System.out.println("Lista de pessoas:");
        for (Pessoa pessoa : pessoas) {
			System.out.println(pessoa);
		}
	}
	
	private static void imprimeDepartamentos(List<Departamento> departamentos) {
		System.out.println("Lista de departamentos:");
        for (Departamento departamento : departamentos) {
			System.out.println(departamento);
		}
	}
}
