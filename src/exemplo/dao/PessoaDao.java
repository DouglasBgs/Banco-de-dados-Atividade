package exemplo.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import exemplo.modelo.Pessoa;

public class PessoaDao implements IDao<Pessoa> {
	public List<Pessoa> getAll() {
		Connection conn = DatabaseAccess.getConnection();
		Statement stmt = null;
		ResultSet rs = null;
		
		List<Pessoa> pessoas = new ArrayList<Pessoa>();
		
		try {
			stmt = conn.createStatement();
			
			String SQL = "SELECT * FROM Pessoa"; // consulta de SELECT
	        rs = stmt.executeQuery(SQL); // executa o SELECT
	        
	        while (rs.next()) {
	        	Pessoa p = getPessoaFromRs(rs);
	        	
	        	pessoas.add(p);
	        }
			
		} catch (SQLException e) {
			throw new RuntimeException("[getAllPessoas] Erro ao selecionar todas as pessoas", e);
		} finally {
			close(conn, stmt, rs);
		}
		
		return pessoas;		
	}
	
	public Pessoa getById(int id) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		Pessoa pessoa = null;
		
		try {
			String SQL = "SELECT * FROM Pessoa WHERE id = ?"; // consulta de SELECT
			stmt = conn.prepareStatement(SQL);
			stmt.setInt(1, id);
			
	        rs = stmt.executeQuery(); // executa o SELECT
	        
	        while (rs.next()) {
	        	pessoa = getPessoaFromRs(rs);
	        }
			
		} catch (SQLException e) {
			throw new RuntimeException("[getPessoaById] Erro ao selecionar a pessoa por id", e);
		} finally {
			close(conn, stmt, rs);
		}
		
		return pessoa;		
	}
	
	public void insert(Pessoa pessoa) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
				
		try {
			String SQL = "INSERT INTO Pessoa (nome, idade) VALUES (?, ?)";
			stmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
	    	stmt.setString(1, pessoa.getNome()); // insira na segunda ? o nome da pessoa
	    	stmt.setInt(2, pessoa.getIdade()); // insira na terceira ? a idade da pessoa
			
	        stmt.executeUpdate(); // executa o SELECT
	        
	        rs = stmt.getGeneratedKeys();
	        
	        if (rs.next()) {
	        	pessoa.setId(rs.getInt(1));
	        }
			
		} catch (SQLException e) {
			throw new RuntimeException("[inserePessoa] Erro ao inserir a pessoa", e);
		} finally {
			close(conn, stmt, rs);
		}
				
	}
	
	public void delete(int id) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
			
		try {
			String SQL = "DELETE Pessoa WHERE id=?";
			stmt = conn.prepareStatement(SQL);
			stmt.setInt(1, id);
			
	        stmt.executeUpdate(); 			
		} catch (SQLException e) {
			throw new RuntimeException("[deletePessoa] Erro ao remover a pessoa por id", e);
		} finally {
			close(conn, stmt, null);
		}
	}
	
	public void update(Pessoa pessoa) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
				
		try {
			String SQL = "UPDATE Pessoa SET nome = ?, idade = ? WHERE id=?";
			stmt = conn.prepareStatement(SQL);
	    	stmt.setString(1, pessoa.getNome()); // insira na primeira ? o nome da pessoa
	    	stmt.setInt(2, pessoa.getIdade()); // insira na segunda ? a idade da pessoa
	    	// insira na última ? o id da pessoa
	    	stmt.setInt(3, pessoa.getId());
	    	
	        stmt.executeUpdate(); // executa o UPDATE			
		} catch (SQLException e) {
			throw new RuntimeException("[updatePessoa] Erro ao atualizar a pessoa", e);
		} finally {
			close(conn, stmt, rs);
		}
				
	}
	
	private Pessoa getPessoaFromRs(ResultSet rs) throws SQLException {
		Pessoa p = new Pessoa(); // cria um objeto de pessoa
		p.setId(rs.getInt("id")); // insere id recuperado do banco na pessoa
		p.setNome(rs.getString("nome")); // insere nome recuperado do banco na pessoa
		p.setIdade(rs.getInt("idade")); // insere idade recuperada do banco na pessoa
		
		return p;
	}
	
	private void close(Connection conn, Statement stmt, ResultSet rs) {
		try {
			if (rs != null) { rs.close(); }
			if (stmt != null) { stmt.close(); }
			if (conn != null) { conn.close(); }
		} catch (SQLException e) {
			throw new RuntimeException("Erro ao fechar recursos.", e);
		}
	}
}
