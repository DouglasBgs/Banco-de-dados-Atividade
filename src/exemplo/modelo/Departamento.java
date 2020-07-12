package exemplo.modelo; 

import java.util.List;

public class Departamento {
	private int id;
	private String nome;
	private List<Pessoa> funcionarios;
	
	public Departamento() { }
	
	public Departamento(int id, String nome, List<Pessoa> funcionarios) {		
		this.id = id;
		this.nome = nome;
		this.funcionarios = funcionarios;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getNome() {
		return nome;
	}

	public void setNome(String nome) {
		this.nome = nome;
	}

	public List<Pessoa> getFuncionarios() {
		return funcionarios;
	}

	public void setFuncionarios(List<Pessoa> funcionarios) {
		this.funcionarios = funcionarios;
	}

	@Override
	public String toString() {
		return "Departamento [id=" + id + ", nome=" + nome + "]";
	}
	
	
}
