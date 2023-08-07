# FlounderIR

FlounderIR is a intermediate representation for compiling SQL queries into machine code, that tackles high compilation times by providing an IR similar to `x86_64` assembly.
By employing lightweight abstractions such as virtual registers with explicit lifetime annotations and C++ function calls, FlounderIR achieves both efficient compilation and user-friendly operation.
FlounderIR was invented by [Henning Funke](https://github.com/Henning1) ([see original source code](https://github.com/Henning1/resql)).
TunaDB includes a rewritten implementation of FlounderIR.

## Library
FlounderIR, as shipped with TunaDB, can be used as a standalone library.
Use `make flounder` to build and include the library ([lib/libflounder.a](lib/libflounder.a)) and header files to your project.

## Code Structure
### IR
The IR's instructions and operands (such as register, memory operands, constants, labels) can be found in [flounder/ir](ir).

### Translation & Compilation
After generating IR instructions, Flounder will translate the instructions into machine code, using [asmjit](https://github.com/asmjit/asmjit).
Before compiling to machine code, however, virtual registers need to be replaced by machine registers and IR instructions need to be translated to asm instructions.
The corresponding code can be found in [flounder/compilation](compilation).

### Optimizer
FlounderIR includes two optimization phases.
The first will run over the emitted Flounder instructions (i.e., with virtual registers), the second will run over assembly instructions with assigned machine registers.
The code of the optimzations is located in [flounder/optimization](flounder/optimization).
