package exemplo.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import exemplo.modelo.Departamento;

public class DepartamentoDao implements IDao<Departamento> {

	public DepartamentoDao() {
		try {
			createTable();
		} catch (SQLException e) {
			//throw new RuntimeException("Erro ao criar tabela departamento");
			e.printStackTrace();
		}
	}
	
	// Cria a tabela se não existir
	private void createTable() throws SQLException {
		final String sqlCreate = "IF NOT EXISTS (" 
				+ "SELECT * FROM sys.tables t JOIN sys.schemas s ON " 
				+ "(t.schema_id = s.schema_id) WHERE s.name = 'dbo'" 
				+ "AND t.name = 'Departamento')"
				+ "CREATE TABLE Departamento"
				+ " (id	int	IDENTITY,"
				+ "  nome	VARCHAR(255),"
				+ "  PRIMARY KEY (id))";
		
		Connection conn = DatabaseAccess.getConnection();
		
		Statement stmt = conn.createStatement();
		stmt.execute(sqlCreate);
	}

	public List<Departamento> getAll() {
		Connection conn = DatabaseAccess.getConnection();
		Statement stmt = null;
		ResultSet rs = null;
		
		List<Departamento> departamentos = new ArrayList<Departamento>();
		
		try {
			stmt = conn.createStatement();
			
			String SQL = "SELECT * FROM Departamento"; // consulta de SELECT
	        rs = stmt.executeQuery(SQL); // executa o SELECT
	        
	        while (rs.next()) {
	        	Departamento d = getDepartamentoFromRs(rs);
	        	
	        	departamentos.add(d);
	        }
			
		} catch (SQLException e) {
			throw new RuntimeException("[getAllDepartamentos] Erro ao selecionar todos os departamentos", e);
		} finally {
			close(conn, stmt, rs);
		}
		
		return departamentos;		
	}
	
	public Departamento getById(int id) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		Departamento departamento = null;
		
		try {
			String SQL = "SELECT * FROM Departamento WHERE id = ?"; // consulta de SELECT
			stmt = conn.prepareStatement(SQL);
			stmt.setInt(1, id);
			
	        rs = stmt.executeQuery(); // executa o SELECT
	        
	        while (rs.next()) {
	        	departamento = getDepartamentoFromRs(rs);
	        }
			
		} catch (SQLException e) {
			throw new RuntimeException("[getDepartamentoById] Erro ao selecionar o departamento por id", e);
		} finally {
			close(conn, stmt, rs);
		}
		
		return departamento;		
	}
	
	public void insert(Departamento departamento) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
				
		try {
			String SQL = "INSERT INTO Departamento (nome) VALUES (?)";
			stmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
	    	stmt.setString(1, departamento.getNome()); // insira na segunda ? o nome da pessoa
	    	
			
	        stmt.executeUpdate(); // executa o SELECT
	        
	        rs = stmt.getGeneratedKeys();
	        
	        if (rs.next()) {
	        	departamento.setId(rs.getInt(1));
	        }
			
		} catch (SQLException e) {
			throw new RuntimeException("[insereDepartamento] Erro ao inserir o departamento", e);
		} finally {
			close(conn, stmt, rs);
		}
				
	}
	
	public void delete(int id) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
			
		try {
			String SQL = "DELETE Departamento WHERE id=?";
			stmt = conn.prepareStatement(SQL);
			stmt.setInt(1, id);
			
	        stmt.executeUpdate(); 			
		} catch (SQLException e) {
			throw new RuntimeException("[deleteDepartamento] Erro ao remover o departamento por id", e);
		} finally {
			close(conn, stmt, null);
		}
	}
	
	public void update(Departamento departamento) {
		Connection conn = DatabaseAccess.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
				
		try {
			String SQL = "UPDATE Departamento SET nome = ? WHERE id=?";
			stmt = conn.prepareStatement(SQL);
	    	stmt.setString(1, departamento.getNome()); // insira na primeira ? o nome da pessoa
	    	// insira na última ? o id da pessoa
	    	stmt.setInt(2, departamento.getId());
	    	
	        stmt.executeUpdate(); // executa o UPDATE			
		} catch (SQLException e) {
			throw new RuntimeException("[updateDepartamento] Erro ao atualizar o departamento", e);
		} finally {
			close(conn, stmt, rs);
		}
				
	}
	
	private Departamento getDepartamentoFromRs(ResultSet rs) throws SQLException {
		Departamento d = new Departamento(); // cria um objeto de pessoa
		d.setId(rs.getInt("id")); // insere id recuperado do banco na pessoa
		d.setNome(rs.getString("nome")); // insere nome recuperado do banco na pessoa
		
		return d;
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
