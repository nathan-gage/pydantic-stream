# Autoresearch Ideas

## Fast Custom Object Skip (High Impact, Complex)

**Idea**: Replace `jiter.next_skip()` for unknown complex values with a custom SIMD-friendly byte scanner.

**Why it matters**:
- "default" projection: 3.1ms is from jiter's per-element dispatch (700 calls per record × 500 records)
- A custom scanner using a lookup table would do O(n_bytes) instead of O(n_elements × overhead)
- Expected savings: ~1.5ms per shape × 2 = 3ms total → 26.96ms → ~24ms

**Implementation plan**:
1. Add `memchr = "2"` to Cargo.toml (already in lockfile)
2. Add `fast_skip_to_object_end(data, start) -> Result<usize>` to projection.rs
   - Uses a const lookup table for structural chars (`"`, `{`, `}`, `[`, `]`, `\`)
   - LLVM should auto-vectorize the lookup-table scan with NEON
3. Add `jiter_base: &mut usize` param to `project_object_inner`, `project_object_from_current`, `process_field`
4. In EARLY_EXIT=true phase (when remaining==0 and we're about to skip):
   - Compute abs_pos = jiter_base + jiter.current_index()
   - fast_skip_to_object_end(input, abs_pos) → abs_end
   - *jiter = Jiter::new(&input[abs_end..]); *jiter_base = abs_end; break
5. Change copy_raw_value to use jiter.slice_to_current(start) (not &input[start..end])
   - This makes copy correct after jiter replacement (values relative to new jiter.data)
6. Update project_array_items_partial: obj_end = pos + jiter_base + jiter.current_index()

**Key correctness insight**:
- `jiter_base` tracks offset of jiter.data start within original `input`
- After replacement: jiter.current_index() is relative to &input[abs_end..], so abs position = jiter_base + jiter.current_index()
- copy_raw_value uses jiter.slice_to_current(start) = &jiter.data[start..current] which is correct regardless of base

**Risks**: Correctness of custom JSON skip (must handle all escape sequences, nesting).

## Profile-Guided Optimization (Medium Impact, Low Effort)
- Build with profile-generate, run benchmarks, build with profile-use
- Maturin supports RUSTFLAGS env var
- Expected: 5-15% improvement from better inlining/layout decisions

## AHash Internal Details
- AHash 0.8 uses AES-NI on ARM64 → already very fast
- No improvement possible here
