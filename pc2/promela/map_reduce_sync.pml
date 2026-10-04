/* 
 * Modelo Promela - Patrón Worker Pool con Local Reduction
 * Curso: CC65 - Programación Concurrente y Distribuida
 * Proyecto: UrbanBus (PC2)
 *
 * Descripción:
 * Este modelo valida la sincronización del algoritmo map-reduce
 * implementado en Go. Modela 3 roles:
 *   1. Producer: Envia N "chunks" de datos y luego señales de fin.
 *   2. Workers (Mappers): Reciben chunks y suman en memoria LOCAL (sin Mutex global).
 *   3. Reducer: Recibe los sub-totales locales y hace el merge final.
 *
 * Invariantes que prueba este modelo:
 * - Absence of Deadlocks: Los canales acotados no se bloquean infinitamente.
 * - Absence of Race Conditions: Los workers solo modifican variables locales.
 * - Correctitud Funcional: La suma final en el Reducer es EXACTAMENTE igual al
 *   total de chunks enviados por el productor (assert final).
 */

#define NUM_WORKERS 3
#define TOTAL_CHUNKS 6
#define EOF_SIGNAL 0
#define DATA_CHUNK 1

/* Canales asíncronos (bufers acotados) */
chan jobs = [3] of { byte };
chan results = [NUM_WORKERS] of { byte };

int final_count = 0; /* Estado global final (sólo modificado por Reducer) */

proctype Producer() {
    byte i = 1;
    
    /* 1. Generación de chunks (representa leer el CSV) */
    do
    :: i <= TOTAL_CHUNKS ->
        jobs ! DATA_CHUNK;
        i++;
    :: i > TOTAL_CHUNKS ->
        break;
    od;

    /* 2. Señal de terminación (equivalente a close(jobs) en Go) */
    i = 1;
    do
    :: i <= NUM_WORKERS ->
        jobs ! EOF_SIGNAL;
        i++;
    :: i > NUM_WORKERS ->
        break;
    od;
}

proctype Worker() {
    byte local_count = 0; /* Memoria local: evita el sync.Mutex */
    byte chunk;

    do
    :: jobs ? chunk ->
        if
        :: chunk == DATA_CHUNK ->
            /* Operación local (representa parsear y agregar en map local) */
            local_count = local_count + 1; 
        :: chunk == EOF_SIGNAL ->
            /* Trabajo terminado */
            break;
        fi;
    od;

    /* Envío del resultado parcial al Reducer */
    results ! local_count;
}

proctype Reducer() {
    byte i = 1;
    byte partial;
    
    /* Espera recibir un resultado por cada worker */
    do
    :: i <= NUM_WORKERS ->
        results ? partial;
        /* Merge final (Sección Crítica pero ejecutada secuencialmente por un solo Goroutine) */
        final_count = final_count + partial;
        i++;
    :: i > NUM_WORKERS ->
        break;
    od;

    /* INVARIANTE CLAVE: La suma de los procesamientos locales debe ser el total exacto */
    assert(final_count == TOTAL_CHUNKS);
}

/* LTL Properties para Verificación Formal (Observación #3) */

/* 1. Liveness: Eventualmente ( <> ), el procesamiento finalizará con el conteo exacto, demostrando ausencia de Deadlocks. */
ltl eventual_completion { <> (final_count == TOTAL_CHUNKS) }

/* 2. Safety (Absence of Race Condition): Siempre ( [] ), los canales se mantienen dentro de límites seguros 
   y el estado global (final_count) solo muta sin interferencia concurrente (por diseño del patrón Worker Pool). */
ltl safe_channels { [] (len(jobs) <= 3 && len(results) <= NUM_WORKERS) }

init {
    atomic {
        run Producer();
        
        byte i = 1;
        do
        :: i <= NUM_WORKERS -> 
            run Worker(); 
            i++;
        :: i > NUM_WORKERS -> 
            break;
        od;

        run Reducer();
    }
}
