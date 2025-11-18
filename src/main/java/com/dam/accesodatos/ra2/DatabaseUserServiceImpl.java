package com.dam.accesodatos.ra2;

import com.dam.accesodatos.config.DatabaseConfig;
import com.dam.accesodatos.model.User;
import com.dam.accesodatos.model.UserCreateDto;
import com.dam.accesodatos.model.UserQueryDto;
import com.dam.accesodatos.model.UserUpdateDto;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementación del servicio JDBC para gestión de usuarios
 *
 * ESTRUCTURA DE IMPLEMENTACIÓN:
 * - ✅ 5 MÉTODOS IMPLEMENTADOS (ejemplos para estudiantes)
 * - ❌ 8 MÉTODOS TODO (estudiantes deben implementar)
 *
 * MÉTODOS IMPLEMENTADOS (Ejemplos):
 * 1. testConnection() - Ejemplo básico de conexión JDBC
 * 2. createUser() - INSERT con PreparedStatement y getGeneratedKeys
 * 3. findUserById() - SELECT y mapeo de ResultSet a objeto
 * 4. updateUser() - UPDATE statement con validación
 * 5. transferData() - Transacción manual con commit/rollback
 *
 * MÉTODOS TODO (Estudiantes implementan):
 * 1. deleteUser()
 * 2. findAll()
 * 3. findUsersByDepartment()
 * 4. searchUsers()
 * 5. batchInsertUsers()
 * 6. getDatabaseInfo()
 * 7. getTableColumns()
 * 8. executeCountByDepartment()
 */
@Service
public class DatabaseUserServiceImpl implements DatabaseUserService {
    private DatabaseConfig dataSource;

    // JDBC PURO - SIN Spring DataSource
    // Los estudiantes usan DatabaseConfig.getConnection() directamente
    // para obtener conexiones usando DriverManager

    // ========== CE2.a: Connection Management ==========

    /**
     * ✅ EJEMPLO IMPLEMENTADO 1/5: Prueba de conexión básica
     *
     * Este método muestra el patrón fundamental de JDBC PURO:
     * 1. Obtener conexión usando DriverManager (vía DatabaseConfig)
     * 2. Ejecutar una query simple
     * 3. Procesar resultados
     * 4. Cerrar recursos con try-with-resources
     */
    @Override
    public String testConnection() {
        // Patrón try-with-resources: cierra automáticamente Connection, Statement, ResultSet
        try (Connection conn = DatabaseConfig.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 as test, DATABASE() as db_name")) {

            // Validar que la conexión está abierta
            if (conn.isClosed()) {
                throw new RuntimeException("La conexión está cerrada");
            }

            // Navegar al primer (y único) resultado
            if (rs.next()) {
                int testValue = rs.getInt("test");
                String dbName = rs.getString("db_name");

                // Obtener información adicional de la conexión
                DatabaseMetaData metaData = conn.getMetaData();
                String dbProduct = metaData.getDatabaseProductName();
                String dbVersion = metaData.getDatabaseProductVersion();

                return String.format("✓ Conexión exitosa a %s %s | Base de datos: %s | Test: %d",
                        dbProduct, dbVersion, dbName, testValue);
            } else {
                throw new RuntimeException("No se obtuvieron resultados de la query de prueba");
            }

        } catch (SQLException e) {
            throw new RuntimeException("Error al probar la conexión: " + e.getMessage(), e);
        }
    }


    // ========== CE2.b: CRUD Operations ==========

    /**
     * ✅ EJEMPLO IMPLEMENTADO 2/5: INSERT con PreparedStatement
     *
     * Este método muestra cómo:
     * - Usar PreparedStatement para prevenir SQL injection
     * - Setear parámetros con tipos específicos
     * - Obtener IDs autogenerados con getGeneratedKeys()
     * - Manejar excepciones SQL
     */
    @Override
    public User createUser(UserCreateDto dto) {
        String sql = "INSERT INTO users (name, email, department, role, active, created_at, updated_at) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            // Setear parámetros del PreparedStatement
            // Índices empiezan en 1, no en 0
            pstmt.setString(1, dto.getName());
            pstmt.setString(2, dto.getEmail());
            pstmt.setString(3, dto.getDepartment());
            pstmt.setString(4, dto.getRole());
            pstmt.setBoolean(5, true); // active por defecto
            pstmt.setTimestamp(6, Timestamp.valueOf(LocalDateTime.now())); // created_at
            pstmt.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now())); // updated_at

            // Ejecutar INSERT y obtener número de filas afectadas
            int affectedRows = pstmt.executeUpdate();

            if (affectedRows == 0) {
                throw new RuntimeException("Error: INSERT no afectó ninguna fila");
            }

            // Obtener el ID autogenerado
            try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    Long generatedId = generatedKeys.getLong(1);

                    // Crear objeto User con el ID generado
                    User newUser = new User(generatedId, dto.getName(), dto.getEmail(),
                            dto.getDepartment(), dto.getRole());
                    newUser.setActive(true);
                    newUser.setCreatedAt(LocalDateTime.now());
                    newUser.setUpdatedAt(LocalDateTime.now());

                    return newUser;
                } else {
                    throw new RuntimeException("Error: INSERT exitoso pero no se generó ID");
                }
            }

        } catch (SQLException e) {
            // Manejar errores específicos como email duplicado
            if (e.getMessage().contains("Unique index or primary key violation")) {
                throw new RuntimeException("Error: El email '" + dto.getEmail() + "' ya está registrado", e);
            }
            throw new RuntimeException("Error al crear usuario: " + e.getMessage(), e);
        }
    }

    /**
     * ✅ EJEMPLO IMPLEMENTADO 3/5: SELECT y mapeo de ResultSet
     *
     * Este método muestra cómo:
     * - Usar PreparedStatement para queries parametrizadas
     * - Navegar ResultSet con rs.next()
     * - Mapear columnas SQL a campos Java
     * - Manejar tipos de datos (Long, String, Boolean, Timestamp)
     */
    @Override
    public User findUserById(Long id) {
        final String sql = "SELECT * FROM users WHERE id = ?";
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    // --- ESTRATEGIA: Usar constructor vacío y setters ---
                    User user = new User(); // 1. Crea un objeto User vacío

                    // 2. Pobla el objeto usando los setters
                    user.setId(rs.getLong("id"));
                    user.setName(rs.getString("name"));
                    user.setEmail(rs.getString("email"));
                    user.setDepartment(rs.getString("department"));
                    user.setRole(rs.getString("role"));
                    user.setActive(rs.getBoolean("active"));
                    user.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
                    user.setUpdatedAt(rs.getTimestamp("updated_at").toLocalDateTime());

                    return user;
                } else {
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error al buscar usuario por ID.", e);
        }
    }

    /**
     * ✅ EJEMPLO IMPLEMENTADO 4/5: UPDATE statement
     *
     * Este método muestra cómo:
     * - Validar que un registro existe antes de actualizar
     * - Construir UPDATE statement con campos opcionales
     * - Actualizar solo los campos proporcionados
     * - Verificar filas afectadas
     */
    @Override
    public User updateUser(Long id, UserUpdateDto dto) {
        // Primero verificar que el usuario existe
        User existing = findUserById(id);
        if (existing == null) {
            throw new RuntimeException("No se encontró usuario con ID " + id);
        }

        // Aplicar actualizaciones del DTO al usuario existente
        dto.applyTo(existing);

        // Construir UPDATE statement
        String sql = "UPDATE users SET name = ?, email = ?, department = ?, role = ?, " +
                     "active = ?, updated_at = ? WHERE id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // Setear todos los parámetros (incluso los no modificados)
            pstmt.setString(1, existing.getName());
            pstmt.setString(2, existing.getEmail());
            pstmt.setString(3, existing.getDepartment());
            pstmt.setString(4, existing.getRole());
            pstmt.setBoolean(5, existing.getActive());
            pstmt.setTimestamp(6, Timestamp.valueOf(LocalDateTime.now()));
            pstmt.setLong(7, id);

            // Ejecutar UPDATE
            int affectedRows = pstmt.executeUpdate();

            if (affectedRows == 0) {
                throw new RuntimeException("Error: UPDATE no afectó ninguna fila");
            }

            // Retornar usuario actualizado
            return findUserById(id);

        } catch (SQLException e) {
            throw new RuntimeException("Error al actualizar usuario con ID " + id + ": " + e.getMessage(), e);
        }
    }

    @Override
    public boolean deleteUser(Long id) {

        final String sql = "DELETE FROM users WHERE id = ?";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setLong(1, id);

            // executeUpdate() retorna el número de filas afectadas.
            int rowsAffected = ps.executeUpdate();

            // Si se afectó una fila (o más, aunque con ID debería ser una),
            // la eliminación fue exitosa. Si fue 0, el usuario no existía.
            return rowsAffected > 0;

        } catch (SQLException e) {
            System.err.println("Error de SQL al eliminar usuario con ID " + id + ": " + e.getMessage());
            throw new RuntimeException("Error de base de datos al eliminar el usuario.", e);
        }

    }

    @Override
    public List<User> findAll() {

        List<User> users = new ArrayList<>();

        // --- CORRECCIÓN CLAVE: Se añade "ORDER BY created_at DESC" a la consulta ---
        // Esto asegura que los usuarios más recientemente creados aparezcan primero en la lista,
        // cumpliendo con el requisito del test.
        final String sql = "SELECT * FROM users ORDER BY created_at DESC";

        try (Connection conn = DatabaseConfig.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                // Se usa el constructor por defecto y los setters para ser compatibles con tu clase User.
                User user = new User();
                user.setId(rs.getLong("id"));
                user.setName(rs.getString("name"));
                user.setEmail(rs.getString("email"));
                user.setDepartment(rs.getString("department"));
                user.setRole(rs.getString("role"));
                user.setActive(rs.getBoolean("active"));
                user.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
                user.setUpdatedAt(rs.getTimestamp("updated_at").toLocalDateTime());

                users.add(user);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error de base de datos al obtener todos los usuarios.", e);
        }

        return users;
    }

    // ========== CE2.c: Advanced Queries ==========

    @Override
    public List<User> findUsersByDepartment(String department) {
        List<User> users = new ArrayList<>();


        final String sql = "SELECT * FROM users WHERE department = ? AND active = true ORDER BY name";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, department);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {

                    User user = new User();
                    user.setId(rs.getLong("id"));
                    user.setName(rs.getString("name"));
                    user.setEmail(rs.getString("email"));
                    user.setDepartment(rs.getString("department"));
                    user.setRole(rs.getString("role"));
                    user.setActive(rs.getBoolean("active"));
                    user.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
                    user.setUpdatedAt(rs.getTimestamp("updated_at").toLocalDateTime());

                    users.add(user);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error de base de datos al buscar usuarios por departamento.", e);
        }

        return users;
    }



    @Override
    public List<User> searchUsers(UserQueryDto query) {
        List<User> users = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT * FROM users WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (query.getDepartment() != null && !query.getDepartment().isEmpty()) {
            sql.append(" AND department = ?");
            params.add(query.getDepartment());
        }
        if (query.getRole() != null && !query.getRole().isEmpty()) {
            sql.append(" AND role = ?");
            params.add(query.getRole());
        }
        // Asumiendo que la columna en la BBDD se llama 'active'
        if (query.getActive() != null) {
            sql.append(" AND active = ?");
            params.add(query.getActive());
        }

        sql.append(" ORDER BY id LIMIT ? OFFSET ?");
        params.add(query.getSize());
        params.add(query.getPage() * query.getSize());

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {

            int paramIndex = 1;
            for (Object param : params) {
                ps.setObject(paramIndex++, param);
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // --- ESTRATEGIA: Usar constructor vacío y setters ---
                    User user = new User();
                    user.setId(rs.getLong("id"));
                    user.setName(rs.getString("name"));
                    user.setEmail(rs.getString("email"));
                    user.setDepartment(rs.getString("department"));
                    user.setRole(rs.getString("role"));
                    user.setActive(rs.getBoolean("active"));
                    user.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
                    user.setUpdatedAt(rs.getTimestamp("updated_at").toLocalDateTime());
                    users.add(user);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error en búsqueda dinámica.", e);
        }
        return users;
    }


    // ========== CE2.d: Transactions ==========

    /**
     * ✅ EJEMPLO IMPLEMENTADO 5/5: Transacción manual con commit/rollback
     *
     * Este método muestra cómo:
     * - Desactivar auto-commit para control manual de transacciones
     * - Realizar múltiples operaciones en una transacción
     * - Hacer commit si todo tiene éxito
     * - Hacer rollback si hay algún error
     * - Restaurar auto-commit al estado original
     */
    @Override
    public boolean transferData(List<User> users) {
        Connection conn = null;

        try {

            conn = DatabaseConfig.getConnection();


            conn.setAutoCommit(false);

            String sql = "INSERT INTO users (name, email, department, role, active, created_at, updated_at) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {

                for (User user : users) {
                    pstmt.setString(1, user.getName());
                    pstmt.setString(2, user.getEmail());
                    pstmt.setString(3, user.getDepartment());
                    pstmt.setString(4, user.getRole());
                    pstmt.setBoolean(5, user.getActive() != null ? user.getActive() : true);
                    pstmt.setTimestamp(6, Timestamp.valueOf(
                            user.getCreatedAt() != null ? user.getCreatedAt() : LocalDateTime.now()));
                    pstmt.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now()));

                    pstmt.executeUpdate();
                }
            }


            conn.commit();

            return true;

        } catch (SQLException e) {

            if (conn != null) {
                try {

                    conn.rollback();
                } catch (SQLException rollbackEx) {
                    throw new RuntimeException("Error crítico en rollback: " + rollbackEx.getMessage(), rollbackEx);
                }
            }

            throw new RuntimeException("Error en transacción, se hizo rollback: " + e.getMessage(), e);

        } finally {

            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {

                    System.err.println("Error al cerrar conexión: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public int batchInsertUsers(List<User> users) {

        if (users == null || users.isEmpty()) {
            return 0;
        }

        // --- CORRECCIÓN 1: La sentencia SQL NO incluye la columna 'id' ---
        // Se asume que la base de datos generará el ID automáticamente.
        // Los nombres de las columnas ('active', 'created_at', etc.) deben coincidir con tu tabla.
        final String sql = "INSERT INTO users (name, email, department, role, active, created_at, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        Connection conn = null;
        try {
            conn = DatabaseConfig.getConnection();

            // Usamos un PreparedStatement dentro del try-with-resources
            try (PreparedStatement ps = conn.prepareStatement(sql)) {

                // Desactivamos auto-commit para un rendimiento óptimo en operaciones batch
                conn.setAutoCommit(false);

                for (User user : users) {
                    // --- CORRECCIÓN 2: Los índices de los parámetros se ajustan ---
                    // Ahora el 'name' es el parámetro 1, 'email' el 2, y así sucesivamente.
                    ps.setString(1, user.getName());
                    ps.setString(2, user.getEmail());
                    ps.setString(3, user.getDepartment());
                    ps.setString(4, user.getRole());
                    ps.setBoolean(5, user.getActive());
                    // Aseguramos que las fechas no sean nulas para evitar errores
                    ps.setTimestamp(6, java.sql.Timestamp.valueOf(
                            user.getCreatedAt() != null ? user.getCreatedAt() : LocalDateTime.now()
                    ));
                    ps.setTimestamp(7, java.sql.Timestamp.valueOf(
                            user.getUpdatedAt() != null ? user.getUpdatedAt() : LocalDateTime.now()
                    ));

                    ps.addBatch();
                }

                int[] results = ps.executeBatch();
                conn.commit();

                // Contamos el número de inserciones exitosas
                int successfulInserts = 0;
                for (int result : results) {
                    // Statement.SUCCESS_NO_INFO es una constante que algunos drivers devuelven
                    // para indicar éxito sin dar un conteo de filas.
                    if (result >= 1 || result == Statement.SUCCESS_NO_INFO) {
                        successfulInserts++;
                    }
                }

                return successfulInserts;
            }
        } catch (SQLException e) {
            // Si algo falla, hacemos rollback de la transacción
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    System.err.println("Error al intentar hacer rollback: " + ex.getMessage());
                }
            }
            throw new RuntimeException("Error de base de datos durante la inserción por lotes.", e);
        } finally {
            // Nos aseguramos de restaurar el modo auto-commit y cerrar la conexión
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {
                    System.err.println("Error al cerrar la conexión: " + e.getMessage());
                }
            }
        }

    }

    // ========== CE2.e: Metadata ==========

    @Override
    public String getDatabaseInfo() {
        try (Connection conn = DatabaseConfig.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            StringBuilder info = new StringBuilder();

            // --- CORRECCIONES EN EL FORMATEO DEL STRING ---
            // Se ajustan las etiquetas para que coincidan con el test.

            info.append("--- Información de la Base de Datos ---\n");

            // Test espera: "H2" y "Base de Datos"
            info.append("Base de Datos: ").append(metaData.getDatabaseProductName()).append(" v").append(metaData.getDatabaseProductVersion()).append("\n");

            // Test espera: "Driver JDBC"
            info.append("Driver JDBC: ").append(metaData.getDriverName()).append(" v").append(metaData.getDriverVersion()).append("\n");

            // Test espera: "URL"
            info.append("URL: ").append(metaData.getURL()).append("\n");

            // Test espera: "Usuario"
            info.append("Usuario: ").append(metaData.getUserName()).append("\n");

            // Test espera: "Soporta Transacciones"
            info.append("Soporta Transacciones: ").append(metaData.supportsTransactions()).append("\n");

            // Test espera: "Soporta Batch"
            info.append("Soporta Batch: ").append(metaData.supportsBatchUpdates()).append("\n");

            int maxConnections = metaData.getMaxConnections();
            info.append("Máximas Conexiones: ").append(maxConnections == 0 ? "Sin límite o no soportado" : maxConnections).append("\n");

            return info.toString();

        } catch (SQLException e) {
            throw new RuntimeException("Error de base de datos al obtener los metadatos.", e);
        }
    }


    @Override
    public List<Map<String, Object>> getTableColumns(String tableName) {
        List<Map<String, Object>> columnsInfo = new ArrayList<>();


        try (Connection conn = DatabaseConfig.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();


            String effectiveTableName = tableName.toUpperCase();


            try (ResultSet rs = metaData.getColumns(null, null, effectiveTableName, null)) {

                while (rs.next()) {
                    Map<String, Object> columnDetails = new HashMap<>();



                    columnDetails.put("name", rs.getString("COLUMN_NAME"));
                    columnDetails.put("typeName", rs.getString("TYPE_NAME"));
                    columnDetails.put("size", rs.getInt("COLUMN_SIZE"));
                    columnDetails.put("nullable", rs.getString("IS_NULLABLE").equalsIgnoreCase("YES"));
                    columnDetails.put("position", rs.getInt("ORDINAL_POSITION"));

                    columnsInfo.add(columnDetails);
                }
            }


        } catch (SQLException e) {
            // Si hay un error de BBDD, lanzamos una excepción para no fallar silenciosamente.
            throw new RuntimeException("Error de base de datos al obtener los metadatos de la tabla " + tableName, e);
        }

        return columnsInfo;
    }

    // ========== CE2.f: Funciones de Agregación ==========

    @Override
    public int executeCountByDepartment(String department) {
        final String sql = "SELECT COUNT(*) FROM users WHERE department = ? AND active = true";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, department);

            try (ResultSet rs = ps.executeQuery()) {
                // Una consulta COUNT(*) siempre devuelve una fila.
                if (rs.next()) {
                    return rs.getInt(1);
                } else {
                    // Este caso es teóricamente imposible para COUNT(*), pero es una buena práctica.
                    return 0;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error de base de datos al contar usuarios.", e);
        }

    }


    // ========== HELPER METHODS ==========

    /**
     * Método auxiliar para mapear ResultSet a objeto User
     *
     * Este método se usa en múltiples lugares para evitar duplicación de código.
     * Extrae todas las columnas del ResultSet y crea un objeto User.
     *
     * @param rs ResultSet posicionado en una fila válida
     * @return User object con datos de la fila
     * @throws SQLException si hay error al leer el ResultSet
     */
    private User mapResultSetToUser(ResultSet rs) throws SQLException {
        User user = new User();

        // Mapear tipos primitivos y objetos
        user.setId(rs.getLong("id"));
        user.setName(rs.getString("name"));
        user.setEmail(rs.getString("email"));
        user.setDepartment(rs.getString("department"));
        user.setRole(rs.getString("role"));
        user.setActive(rs.getBoolean("active"));

        // Mapear Timestamps a LocalDateTime
        Timestamp createdAtTimestamp = rs.getTimestamp("created_at");
        if (createdAtTimestamp != null) {
            user.setCreatedAt(createdAtTimestamp.toLocalDateTime());
        }

        Timestamp updatedAtTimestamp = rs.getTimestamp("updated_at");
        if (updatedAtTimestamp != null) {
            user.setUpdatedAt(updatedAtTimestamp.toLocalDateTime());
        }

        return user;
    }
}
