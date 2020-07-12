package exemplo.modelo; 

public class Carro {
	private int id;
	private String proprietario;
	private String placa;

	public Carro() { }
	
	public Carro(int id, String proprietario, String placa) {
		this.id = id;
		this.proprietario = proprietario;
		this.placa = placa;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getProprietario() {
		return proprietario;
	}

	public void setProprietario(String proprietario) {
		this.proprietario = proprietario;
	}

	public String getPlaca() {
		return placa;
	}

	public void setPlaca(String placa) {
		this.placa = placa;
	}

	@Override
	public String toString() {
		return "Carro [id=" + id + ", proprietario=" + proprietario + ", placa=" + placa + "]";
	}
	
	
}
